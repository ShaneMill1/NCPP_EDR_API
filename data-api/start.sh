#!/bin/bash
#crontab /crontab.txt
#./build_search_engine.sh

#set up dask scheduler
/opt/conda/envs/env/bin/dask-scheduler --port 5500 --dashboard-address '0.0.0.0:5510' & 

#set up 12 dask workers
/opt/conda/envs/env/bin/dask-worker 0.0.0.0:5500 &
/opt/conda/envs/env/bin/dask-worker 0.0.0.0:5500 &
/opt/conda/envs/env/bin/dask-worker 0.0.0.0:5500 &
/opt/conda/envs/env/bin/dask-worker 0.0.0.0:5500 &
/opt/conda/envs/env/bin/dask-worker 0.0.0.0:5500 &
/opt/conda/envs/env/bin/dask-worker 0.0.0.0:5500 &
/opt/conda/envs/env/bin/dask-worker 0.0.0.0:5500 &
/opt/conda/envs/env/bin/dask-worker 0.0.0.0:5500 &
/opt/conda/envs/env/bin/dask-worker 0.0.0.0:5500 &
/opt/conda/envs/env/bin/dask-worker 0.0.0.0:5500 &
/opt/conda/envs/env/bin/dask-worker 0.0.0.0:5500 &
/opt/conda/envs/env/bin/dask-worker 0.0.0.0:5500 &




#set up gunicorn for web service front end with 6 workers
/opt/conda/envs/env/bin/gunicorn -w 6 -t 0 -b 0.0.0.0:5010 EDR.edr_point_server:app --limit-request-line 100000
