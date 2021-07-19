FROM java:8

COPY . IGINX
ENV port 6888
ENV restPort 6666

CMD ["./IGINX/startIginX.sh"]