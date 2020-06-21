#!/bin/sh
exec docker run -d --name dynamodb -p 2000:8000 amazon/dynamodb-local
