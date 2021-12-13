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
#COPY ./dask-worker-space /dask-worker-space
RUN mkdir /dask-worker-space
COPY ./nwm_times.csv /nwm_times.csv

RUN cd /EDR/search_engine/ogc_csw/ && python setup.py build && python setup.py install

RUN chmod +x /start.sh
RUN chmod +x /ndfd_download.sh

RUN groupadd smill
RUN useradd -r -u 1001 -g smill smill
RUN mkdir /home/smill
RUN mkdir /data
RUN chown -R smill /EDR /data /home/smill /var /tmp
RUN chgrp -R smill /EDR /data /home/smill
RUN chmod 775 -R /EDR /data /home /var /tmp /crontab.txt /build_search_engine.sh /start.sh /nwm_times.csv 
RUN chmod 777 -R  /dask-worker-space
WORKDIR /
#COPY ./degrib /degrib
#RUN chmod 755 /degrib
USER smill

EXPOSE 5010
EXPOSE 443

CMD ["./start.sh"]


