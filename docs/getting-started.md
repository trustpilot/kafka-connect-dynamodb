## Getting started

These instructions will get you up and running this connector on your own machine. 
> Check official [Confluent documentation](https://docs.confluent.io/current/connect/userguide.html#installing-plugins) on how to deploy to production cluster.

### Prerequisites

* Install Java 8
* Setup [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)
  * Configured credentials must be able to read and create DynamoDB tables 
* Download Confluent Platform (>=4.1.0) from https://www.confluent.io/download/
* Download latest plugin .jar from releases section
* Make sure you have jq installed https://stedolan.github.io/jq/download/
  
### Setting up DynamoDB table

* Login to AWS console UI
* Create `test-dynamodb-connector` DynamoDB table (or use any other name you choose)
* Enable DynamoDB streams with mode `new image` or `new and old image`
* Set TAG's:
  * `environment=dev`
  * `datalake-ingest=`
* Put some random test data into it

### Running connector

* First we need to perform some configuration changes to make connector package available to Kafka Connect:

  * Store downloaded connector jar file to a location in your filesystem. For instance: `/opt/connectors/kafka-dynamodb-connector`
  * Edit file `${CONFLUENT_PLATFORM_HOME}/etc/schema-registry/connect-avro-distributed.properties` and set `plugin.path=/opt/connectors`

* Next start confluent and configure actual connector, by executing: 
* 
```bash
cd ${CONFLUENT_PLATFORM_HOME}/bin

# Starts all the required services including Kafka and Confluent Connect
./confluent start

# Check if "com.trustpilot.connector.dynamodb.DynamoDBSourceConnector" has been loaded
curl localhost:8083/connector-plugins | jq

# Configure connector
curl -X PUT -H "Content-Type: application/json" --data '{"connector.class":"DynamoDBSourceConnector","tasks.max":"100","name":"myDynamodbConnector"}' localhost:8083/connectors/myDynamodbConnector/config

# Check connector status
curl localhost:8083/connectors/myDynamodbConnector/status | jq

# To track plugin progress and debug issues use
./confluent log connect

# After about ~1 minute your data should be available in Kafka topic. Test it out with:
./kafka-avro-console-consumer --topic dynamodb-test-dynamodb-connector --bootstrap-server localhost:9092  --from-beginning  

# Once finished execute 
./confluent destroy

# Note: don't forget to delete DynamoDB tables
```

> more details http://docs.confluent.io/current/connect/quickstart.html