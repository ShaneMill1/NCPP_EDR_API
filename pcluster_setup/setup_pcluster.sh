#!/bin/bash

pcluster create-cluster --cluster-configuration cluster-config.yaml --cluster-name edr-cluster --region us-west-2
pcluster ssh -n edr-cluster -i /home/ec2-user/.ssh/pcluster_key.pem

#Install Git

yum install git

#Install Docker

sudo yum update -y
sudo amazon-linux-extras install docker
sudo yum install docker
sudo service docker start
sudo systemctl enable docker
sudo usermod -a -G docker ec2-user

#Create Miniconda Environment

wget "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh" 
chmod +x Miniconda3-latest-Linux-x86_64.sh
./Miniconda3-latest-Linux-x86_64.sh

git clone https://github.com/ShaneMill1/NCPP_EDR_API.git

conda env create --file /home/ec2-user/NCPP_EDR_API/data-api/env.yml
conda activate env

#Forward Port 80 to 5400

iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 5400

#Deployment of Gunicorn Server on Host

/home/ec2-user/NCPP_EDR_API/data-api/start.sh
