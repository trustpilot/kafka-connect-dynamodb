
# Connector options

## Minimal configuration

```json

{
    "connector.class": "DynamoDBSourceConnector",
    "tasks.max": "100"
}
```


## All config options (with default values)
```json
{
    "connector.class": "DynamoDBSourceConnector",

    "aws.region": "eu-west-1",
    "aws.access.key.id": "",
    "aws.secret.key": "",
    "aws.assume.role.arn": "",

    "dynamodb.table.env.tag.key": "environment",
    "dynamodb.table.env.tag.value": "dev",
    "dynamodb.table.ingestion.tag.key": "datalake-ingest",
    "dynamodb.table.whitelist": "",
    "dynamodb.service.endpoint": "",

    "kcl.table.billing.mode": "PROVISIONED",

    "resource.tagging.service.endpoint": "",
    
    "kafka.topic.prefix": "dynamodb-",
    "tasks.max": "1",

    "init.sync.delay.period": 60,
    "connect.dynamodb.rediscovery.period": "60000"
}
```
`aws.assume.role.arn` - ARN identifier of an IAM role that the KCL and Dynamo Clients can assume for cross account access

`dynamodb.table.env.tag.key` - tag key used to define environment. Useful if you have `staging` and `production` under same AWS account. Or if you want to use different Kafka Connect clusters to sync different tables.

`dynamodb.table.env.tag.value` - defines from which environment to ingest tables. For e.g. 'staging' or 'production'...

`dynamodb.table.ingestion.tag.key` - only tables marked with this tag key will be ingested.

`kafka.topic.prefix` - all topics created by this connector will have this prefix in their name. Following this pattern `{prefix}-{dynamodb-table-name}`

`tasks.max` - **MUST** always exceed number of tables found for tracking. If max tasks count is lower then found tables count, no tasks will be started!

`init.sync.delay.period` - time interval in seconds. Defines how long `INIT_SYNC` should delay execution before starting. This is used to give time for Kafka Connect tasks to calm down after rebalance (Since multiple tasks rebalances can happen in quick succession and this would mean more duplicated data since `INIT_SYNC` process won't have time mark it's progress). 

`connect.dynamodb.rediscovery.period` - time interval in milliseconds. Defines how often connector should try to find new DynamoDB tables (or detect removed ones). If changes are found tasks are automatically reconfigured.

`dynamodb.service.endpoint` - AWS DynamoDB API Endpoint. Will use default AWS if not set.

`resource.tagging.service.endpoint` - AWS Resource Group Tag API Endpoint. Will use default AWS if not set.

`kcl.table.billing.mode` - Define billing mode for internal table created by the KCL library. Default is provisioned.

`dynamodb.table.whitelist` - Define whitelist of dynamodb table names. This overrides table auto-discovery by ingestion tag.