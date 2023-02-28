# base image
FROM quay.io/opendatahub-contrib/pyspark:s3.3.0-h3.3.3_latest

# define spark and hadoop versions
ENV SPARK_VERSION=3.3.0
ENV HADOOP_VERSION=3.3.3

ADD requirements.txt .
RUN python -m pip install -r requirements.txt

COPY pcap_Conn.py /home

#USER root
#RUN chmod a+rwx /home/pcap_Conn.py


