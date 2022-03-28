import subprocess
import os
import time

from dask.distributed import Client, LocalCluster
from dask_jobqueue import SLURMCluster
import os

cluster=SLURMCluster(header_skip=['--mem'],cores=2,memory="4GB",death_timeout=200,scheduler_options={'host': '0.0.0.0:5000'})
cluster.adapt(minimum_jobs=6,maximum_jobs=50)

os.environ['tcp_scheculer']=cluster.scheduler_address

os.environ['EDR_CONFIG']="./EDR/config/config_pcluster.yml"
subprocess.run("/home/ec2-user/miniconda3/envs/env/bin/gunicorn -w 6 -b 0.0.0.0:5400 EDR.edr_point_server:app --limit-request-line 100000 --timeout 960",shell=True,check=True)

