#!/bin/bash

export MAVEN_OPTS="-Xmx10g -Xms8g"

cd luzzu-communications
mvn exec:java;
