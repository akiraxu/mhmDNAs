#!/bin/bash
docker rm -f mhmdnas
docker build -t mhmdnas:latest .
docker run --name mhmdnas -p 8080:8080 -d mhmdnas