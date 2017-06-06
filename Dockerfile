FROM busybox
RUN wget -O- "http://s3.amazonaws.com/babl/babl_linux_amd64.gz" | gunzip > /bin/babl && chmod +x /bin/babl
ADD babl-errors_linux_amd64 /bin/babl-errors
CMD ["/bin/babl-errors"]
