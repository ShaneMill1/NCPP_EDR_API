docker build -t edr-api-prem .
#docker run -d -v /mnt/data-api/:/data/ -u 2026:2026  -p 5010:5010 edr-api-prem
docker run -d --restart always -v /edr-data/edr-test-data/:/data/ -p 5010:5010 edr-api-prem
