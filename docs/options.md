
# Connector options

## Minimal configuration

```json

{
    "connector.class": "com.trustpilot.connector.dynamodb.DynamoDBSourceConnector",
    "tasks.max": "100"
}
```


## All config options (with default values)
```json
{
    "connector.class": "com.trustpilot.connector.dynamodb.DynamoDBSourceConnector",

    "aws.region": "eu-west-1",
    "aws.access.key.id": "",
    "aws.secret.key": "",

    "dynamodb.table.env.tag.key": "stack",
    "dynamodb.table.env.tag.value": "dev",
    "dynamodb.table.ingestion.tag.key": "datalake-ingest",

    "kafka.topic.prefix": "dynamodb-",
    "tasks.max": "1",

    "init.sync.delay.period": 60,
    "connect.dynamodb.rediscovery.period": "60000"
}
```
`dynamodb.table.env.tag.key` - tag key used to define environment(stack). Useful if you have `staging` and `production` under same AWS account. Or if you want to use different Kafka Connect clusters to sync different tables.

`dynamodb.table.env.tag.value` - defines from which environment or stack to ingest tables. For e.g. 'staging' or 'production'...

`dynamodb.table.ingestion.tag.key` - only tables marked with this tag key will be ingested.

`kafka.topic.prefix` - all topics create by this connector will have this prefix in their name. Following this pattern `{prefix}-{dynamodb-table-name}`

`tasks.max` - **MUST** always exceed number of tables found for tracking. If max tasks count is lower then found tables count, no tasks will be started!

`init.sync.delay.period` - time value in seconds. Defines how long `INIT_SYNC` should delay execution before starting. This is used to give time for Kafka Connect tasks to calm down after rebalance (Since multiple tasks rebalances can happen in quick succession and this would mean more duplicated data since `INIT_SYNC` process won't have time mark it's progress). 

 `connect.dynamodb.rediscovery.period` - time interval in milliseconds. Defines how often connector should try to find new DynamoDB tables (or detect removed ones). If changes are found tasks are automatically reconfigured.




