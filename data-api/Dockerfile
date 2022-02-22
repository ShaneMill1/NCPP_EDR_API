FROM continuumio/miniconda3
RUN apt-get update && apt-get -y install acl vim curl bzip2 cron libzstd-dev libssl-dev libopenblas-base libopenblas-dev libproj-dev proj-data proj-bin libgeos-dev munge

COPY ./env.yml /env.yml
RUN conda update -n base -c defaults conda
RUN mkdir /miniconda
RUN chmod 755 -R /miniconda
RUN conda env create --file env.yml --prefix /conda/env
ENV FLASK_APP=/EDR/edr_point_server.py
ENV EDR_CONFIG=/EDR/config/config.yml

COPY ./ndfd_download.py /ndfd_download.py
COPY ./ndfd_download.sh /ndfd_download.sh
COPY ./crontab.txt /crontab.txt
COPY ./start.sh /start.sh
COPY EDR /EDR
COPY ./build_search_engine.sh /build_search_engine.sh
RUN mkdir /dask-worker-space
COPY ./nwm_times.csv /nwm_times.csv

RUN cd /EDR/search_engine/ogc_csw/ && python setup.py build && python setup.py install

RUN chmod +x /start.sh
RUN chmod +x /ndfd_download.sh

RUN groupadd ec2-user
RUN useradd -r -u 1000 -g ec2-user ec2-user
RUN mkdir /home/ec2-user

#slurm:x:401:401:slurm user:/home/slurm:/bin/bash
RUN groupadd slurm
RUN useradd -r -u 401 -g slurm slurm

#munge:x:402:402:munge user:/home/munge:/sbin/nologin
#RUN groupadd munge
#RUN useradd -r -u 402 -g munge munge
#RUN mkdir /home/munge

RUN mkdir /data
RUN chown -R slurm /EDR /data /var /tmp
RUN chgrp -R slurm /EDR /data
RUN chmod 775 -R /EDR /data /home /var /tmp /crontab.txt /build_search_engine.sh /start.sh /nwm_times.csv 
RUN chmod 777 -R  /dask-worker-space
WORKDIR /

ENV PATH "/usr/lib64/qt-3.3/bin:/opt/amazon/openmpi/bin:/opt/amazon/efa/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/bin:/opt/aws/bin:/opt/parallelcluster/pyenv/versions/3.7.10/envs/awsbatch_virtualenv/bin:/opt/slurm/bin:/home/ec2-user/.local/bin:/home/ec2-user/bin"

USER slurm

EXPOSE 5010
EXPOSE 443
EXPOSE 8787

CMD ["./start.sh"]


