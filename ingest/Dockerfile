FROM continuumio/miniconda3

RUN apt-get update && apt-get -y install rsync acl vim curl bzip2 cron libzstd-dev libssl-dev libopenblas-base libopenblas-dev



COPY ./crontab.txt /crontab.txt
COPY ./automated_ingest /automated_ingest
COPY ./env.yml /env.yml
COPY ./start.sh /start.sh


RUN conda update -n base -c defaults conda
RUN conda env update --file env.yml 

RUN groupadd smill
RUN useradd -r -u 1001 -g smill smill
RUN mkdir /home/smill
RUN mkdir /data
RUN chown -R smill /automated_ingest /data /home/smill /var /tmp
RUN chgrp -R smill /automated_ingest /data /home/smill
RUN chmod 775 -R /automated_ingest /data /home /var /tmp /crontab.txt /start.sh
WORKDIR /

ENV TZ=America/New_York
# Make RUN commands use the new environment:
SHELL ["conda", "run", "-n", "env", "/bin/bash", "-c"]
CMD ["./start.sh"]

