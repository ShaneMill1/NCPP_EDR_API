#!/bin/bash
python bin/pycsw-admin.py -y -c delete_records -f default.cfg
python bin/pycsw-admin.py -c load_records -f default.cfg -p ../records
