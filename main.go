package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/larskluge/babl-server/kafka"
	. "github.com/larskluge/babl-server/utils"
)

//Warning log level

//Broker url
const (
	Version     = "0.0.2"
	TopicEvents = "logs.raw"
)

var (
	executed  = regexp.MustCompile("(?i)req-executed")
	bablError = regexp.MustCompile("(?i)Babl::ModuleError")
)

type Msg struct {
	Hostname         string          `json:"_HOSTNAME"`
	SystemdUnit      string          `json:"_SYSTEMD_UNIT"`
	SyslogIdentifier string          `json:"SYSLOG_IDENTIFIER"`
	ContainerName    string          `json:"CONTAINER_NAME"`
	MessageRaw       json.RawMessage `json:"MESSAGE"`
	Message          string
}

func main() {
	log.SetOutput(os.Stderr)
	log.SetFormatter(&log.JSONFormatter{})

	app := configureCli()
	app.Run(os.Args)
}

func run(kafkaBrokers string, dbg bool) {

	if dbg {
		log.SetLevel(log.DebugLevel)
	}
	brokers := strings.Split(kafkaBrokers, ",")
	Cluster := SplitFirst(kafkaBrokers, ".")
	ParseEvents(Cluster, brokers)

}

func ParseEvents(Cluster string, brokers []string) {

	client := *kafka.NewClient(brokers, "babl-errors", true)
	defer client.Close()

	consumer, err := sarama.NewConsumerFromClient(client)
	Check(err)
	defer consumer.Close()

	offsetNewest, err := client.GetOffset(TopicEvents, 0, sarama.OffsetNewest)
	Check(err)

	cp, err := consumer.ConsumePartition(TopicEvents, 0, offsetNewest)
	Check(err)
	defer cp.Close()
	for msg := range cp.Messages() {
		var m Msg
		var data map[string]interface{}

		err := json.Unmarshal(msg.Value, &m)
		Check(err)
		decode(&m)

		err = json.Unmarshal([]byte(m.Message), &data)
		if err == nil {
			Code := data["code"]
			Status := data["status"]
			Stderr := data["stderr"]
			Module := data["module"]

			if Stderr != nil && bablError.MatchString(Stderr.(string)) {
				Stderr = "Babl::ModuleError"
			}

			// catch execution errors
			if Code == "req-executed" && Status != "SUCCESS" {
				str := fmt.Sprintf("[%s] %s --> %s: %s", Cluster, stripContainerName(m.ContainerName), Status, Stderr)
				notify(Cluster, str)
			}
			//catch execution canceling
			if Code == "req-execution-canceled" && Status != "SUCCESS" {
				str := fmt.Sprintf("[%s] %s --> %s", Cluster, stripContainerName(m.ContainerName), "EXECUTION CANCELED")
				notify(Cluster, str)
			}
			//catch global module timeout
			if Code == "completed" && Status == "MODULE_RESPONSE_TIMEOUT" {
				str := fmt.Sprintf("[%s] %s --> %s", Cluster, Module, Status)
				notify(Cluster, str)
			}
		}
	}
}

func stripContainerName(c string) string {
	n := strings.LastIndex(c, ".")
	return c[0:n]
}

func decode(m *Msg) {
	// MESSAGE can be a string or []byte which represents a string; bug in journald/kafka-manager somehow
	var s string
	err := json.Unmarshal(m.MessageRaw, &s)
	if err == nil {
		m.Message = s
	} else {
		var n []byte
		err = json.Unmarshal(m.MessageRaw, &n)
		Check(err)
		m.Message = string(n)
	}

}

func notify(Cluster string, Msg string) {
	log.WithFields(log.Fields{"cluster": Cluster, "message": Msg}).Info("Module Error Event")
	args := []string{"-c", "sandbox.babl.sh:4445", "babl/events", "-e", "EVENT=babl:error"}
	cmd := exec.Command("/bin/babl", args...)
	cmd.Stdin = strings.NewReader(Msg)
	err := cmd.Run()
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Warn("No babl/events module?")
	}
}
