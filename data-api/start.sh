#!/bin/bash


export EDR_CONFIG=./EDR/config/config_pcluster.yml
/home/ec2-user/miniconda3/envs/env/bin/gunicorn -w 1 -t 6 -b 0.0.0.0:5400 EDR.edr_point_server:app --limit-request-line 100000 --timeout 60 --daemon
