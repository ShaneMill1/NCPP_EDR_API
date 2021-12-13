docker build -t edr-api-ingest-processes .
docker run -d --restart always -v /edr-data/edr-test-data/:/data/ edr-api-ingest-processes
