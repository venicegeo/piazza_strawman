#!/bin/sh

wget http://apache.claz.org/kafka/0.8.2.1/kafka_2.10-0.8.2.1.tgz -c -O ansible/roles/kafka/files/kafka_2.10-0.8.2.1.tgz
wget 'http://downloads.sourceforge.net/project/geoserver/GeoServer/2.8.0/geoserver-2.8.0-war.zip?r=http%3A%2F%2Fgeoserver.org%2Frelease%2Fstable%2F&ts=1445537667&use_mirror=tcpdiag' -c -O ansible/roles/geoserver/files/geoserver-2.8.0-war.zip
