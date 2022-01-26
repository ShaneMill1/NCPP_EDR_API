FROM continuumio/miniconda3

RUN apt-get update && apt-get -y install rsync acl vim curl bzip2 cron libzstd-dev libssl-dev libopenblas-base libopenblas-dev



COPY ./crontab.txt /crontab.txt
COPY ./automated_ingest /automated_ingest
COPY ./env.yml /env.yml
COPY ./start.sh /start.sh


RUN conda update -n base -c defaults conda
RUN conda env update --file env.yml 

RUN groupadd ec2-user
RUN useradd -r -u 1001 -g ec2-user ec2-user
RUN mkdir /home/ec2-user
RUN mkdir /data
RUN chown -R ec2-user /automated_ingest /data /home/ec2-user /var /tmp
RUN chgrp -R ec2-user /automated_ingest /data /home/ec2-user
RUN chmod 775 -R /automated_ingest /data /home /var /tmp /crontab.txt /start.sh
WORKDIR /
USER ec2-user

ENV TZ=America/New_York

# Make RUN commands use the new environment:
SHELL ["conda", "run", "-n", "env", "/bin/bash", "-c"]
CMD ["./start.sh"]

