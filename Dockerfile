FROM debian:latest

RUN apt-get update
RUN apt-get -y install python-dev python-pip
RUN pip install numpy==1.10.1 && pip install pandas==0.17.0
RUN pip install qcache

EXPOSE 9401 9402 9403 9404 9405 9406 9407 9408

ENV QCACHE_PORT 9401

# The start container like this:
# - docker run -p 9401:9401 qcache
# - docker run --env QCACHE_PORT=9402 -p 9402:9402 qcache
CMD [ "sh", "-c", "qcache -p $QCACHE_PORT"]
