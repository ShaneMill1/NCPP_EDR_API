FROM continuumio/miniconda3
RUN apt-get update && apt-get -y install acl vim curl bzip2 cron libzstd-dev libssl-dev libopenblas-base libopenblas-dev libproj-dev proj-data proj-bin libgeos-dev

COPY ./env.yml /env.yml
RUN conda env create --file env.yml
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
RUN useradd -r -u 1001 -g ec2-user ec2-user
RUN mkdir /home/ec2-user
RUN mkdir /data
RUN chown -R ec2-user /EDR /data /home/ec2-user /var /tmp
RUN chgrp -R ec2-user /EDR /data /home/ec2-user
RUN chmod 775 -R /EDR /data /home /var /tmp /crontab.txt /build_search_engine.sh /start.sh /nwm_times.csv 
RUN chmod 777 -R  /dask-worker-space
WORKDIR /
USER ec2-user

EXPOSE 5010
EXPOSE 443

CMD ["./start.sh"]


