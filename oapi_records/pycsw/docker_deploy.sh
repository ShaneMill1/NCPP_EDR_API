#!/bin/bash

#git clone https://github.com/geopython/pycsw.git
docker build -t pycsw .

docker run \
    --name pycsw-dev \
    --detach \
    --volume ${PWD}/pycsw:/usr/lib/python3.5/site-packages/pycsw \
    --volume ${PWD}/docs:/home/pycsw/docs \
    --volume ${PWD}/VERSION.txt:/home/pycsw/VERSION.txt \
    --volume ${PWD}/LICENSE.txt:/home/pycsw/LICENSE.txt \
    --volume ${PWD}/COMMITTERS.txt:/home/pycsw/COMMITTERS.txt \
    --volume ${PWD}/CONTRIBUTING.rst:/home/pycsw/CONTRIBUTING.rst \
    --volume ${PWD}/pycsw/plugins:/home/pycsw/pycsw/plugins \
    --publish 5011:8000 \
    pycsw --reload

# install additional dependencies used in tests and docs
#docker exec \
#    -ti \
#    --user root \
#    pycsw-dev pip3 install -r requirements-dev.txt

# run tests (for example unit tests)
#docker exec -ti pycsw-dev py.test -m unit

# build docs
#docker exec -ti pycsw-dev sh -c "cd docs && make html"
