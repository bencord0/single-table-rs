#!/bin/sh
docker rm -f dynamodb
docker run -d --name dynamodb -p 2000:8000 amazon/dynamodb-local

echo "      export AWS_ENDPOINT_URL=http://localhost:2000"
