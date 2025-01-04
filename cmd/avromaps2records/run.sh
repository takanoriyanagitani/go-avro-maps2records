#!/bin/sh

genavro(){
	cat ./sample.d/sample.jsonl |
		jsons2maps2avro |
		cat > ./sample.d/sample.avro
}

#genavro

export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc

cat ./sample.d/sample.avro |
	./avromaps2records |
	rq -aJ |
	jq -c
