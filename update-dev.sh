#!/bin/bash

name="stremio-jackett-dev"

docker build -t $name .
docker rm -f $name
docker run -p 3001:3000 --net streaming_net --name $name $name