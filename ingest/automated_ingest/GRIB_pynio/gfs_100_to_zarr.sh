#!/bin/bash

#example: gfs_100 00 ./data

./download_model_data.py gfs_100 $1 $2
./create_collections_pynio.py  gfs_100 $1 $2
