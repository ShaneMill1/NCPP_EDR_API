This deploys the EDR API onto the data-api VM

run ./docker.cmd from the command line. This will build the image, run the container

enter the docker container and start the cron

docker exec -it -u 0 <container id> /bin/bash
service cron start
