FROM busybox
ADD babl-errors_linux_amd64 /bin/babl-errors
ADD babl_linux_amd64 /bin/babl
CMD ["/bin/babl-errors"]