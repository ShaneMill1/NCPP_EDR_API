#!/bin/bash
service cron start
crontab /crontab.txt
service cron reload
sleep infinity
