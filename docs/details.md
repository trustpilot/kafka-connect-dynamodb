
# Main states

### 1. "DISCOVERY"

This connector can sync multiple DynamoDB tables at the same time and it does so without requiring explicit configuration for each one. On start and at regular time intervals (by default 60s) after, it queries AWS api for DynamoDB tables which match following criteria and starts Kafka Connect task for each of them:
* ingestion TAG key set
* environment TAG key and value set
* DynamoDB streams enabled (in `new_image` or `new_and_old_image` mode)

> Note: if `dynamodb.table.whitelist` parameter is set, then auto-discovery will not be executed and replication will be issued for explicitly defined tables.
### 2. "INIT_SYNC"

`INIT_SYNC` is a process when all existing table data is scanned and pushed into Kafka destination topic. Usually this happens only once after source task for specific table is started for the first time. But it can be repeated in case of unexpected issues, e.g. if source connector was down for long period of time and it is possible that it has missed some of the change events from the table stream (DynamoDB streams store data for 24 hours only). 

### 3. "SYNC"

Once `INIT_SYNC` is finished source task switches into DynamoDB Streams consumer state. There all changes that happen to the source table are represented in this stream and copied over to the Kafka's destination topic. Consumers of this topic can recreate full state of the source table at any given time.

# How does it work

This connector depends on Kafka Connect framework for most tasks related to Kafka and uses Kinesis Client Library(KCL) + DynamoDB Streams Adapter libraries for DynamoDB Streams consumption. 

Read the following articles to familiarize yourself with them:
* [Connector Developer Guide](https://docs.confluent.io/current/connect/devguide.html)
* [KCL](https://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-kcl.html)
* [KCL DynamoDB adapter](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.KCLAdapter.html)

At it's core this connector starts one Kafka Connect task for each table it syncs. And each task starts a dedicated KCL(Kinesis Consumer Library) worker to read data from the stream. 

## State tracking 

Connector tracks it's state at all stages and is able to continue where it stopped after restart. However state and progress **tracking happens at regular intervals** and not after each processed event, meaning that there can and **will be event duplicates in destination topic**!

Since we are using two different frameworks/libraries together there are two different ways how each of them stores state:
* Kafka connect leverages dedicated `state` topics where connector tasks can push offsets(state) for each partition they are consuming. This connector has no support for source table "partitions" and only one task is allowed to consume one table at a time, therefore it uses table name as partition key and leverage `offsets` dictionary to store tasks state and progress of that state.
* KCL library uses separate dedicated DynamoDB table for each DynamoDB Stream it tracks to remember it's own progress. Since we can only say that message has been consumed once it's delivered to Kafka special synchronization logic is implemented in this connector.
  
> NOTE: KCL library uses `state` table in DynamoDB for each stream it tracks and this table is created **automatically** if it doesn't exist.

### `DISCOVERY` state and task configuration

If `dynamodb.table.whitelist` parameter is not defined connector uses AWS resource group API to receive a list of DynamoDB tables which have ingestion TAG defined. Then it iterates over this list and checks if environment TAG is matched and streams are actually enabled. Connect task is started for each table which meats all requirements.

`discovery` phase is executed on start and every 60 seconds(default config value) after initial start. 

Each started task can be in one of the following states:

 #### `INIT_SYNC` state 

During `INIT_SYNC` phase all records from source table are scanned in batches. After that each batches `EXCLUSIVE_START_KEY` is set as offset data with each record. In case of restart `INIT_SYNC` will continues from this start key. Once all records have been read `INIT_SYNC` is marked as finished in offsets and `SYNC` mode starts. 

> NOTE: On start `INIT_SYNC` is delayed by configurable amount of time (by default 60s). This is done to give connect cluster time to settle down after restart and helps to lower amount of duplicates because of connect task rebalances.

#### `SYNC` state

After `INIT_SYNC` connector starts reading messages from DynamoDB Stream. As first step it makes sure to drop all events which happened before `INIT_SYNC` was started (except for those created during last hour before `INIT_SYNC`). This is done to prevent unnecessary duplicate events(since we already have latest state) and to advance KCL reader into `save zone`. 

Events are considered to be in `save zone` if they there create no earlier then -20 hours before `now`. Otherwise connector has no way to validate that it hasn't skipped some of the events and it has to initiate `INIT_SYNC`!

> NOTE: DynamoDB Streams store data for 24hours only
