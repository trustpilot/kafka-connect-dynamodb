
# Main states

### 1. Source tables discovery & Source tasks configuration

This plugin can sync multiple DynamoDB tables at the same time and it does so without requiring explicit configuration for each one. On start and at regular intervals after that(by default 60s) it queries AWS api for tables which match following criteria and starts Kafka Connect task for each of them:
* table must have configured ingestion TAG key set
* table mush have configured stack(environment) TAG key and value set
* table must have DynamoDB streams enabled (in `new_image` or `new_and_old_image` mode)


### 2. "INIT_SYNC"

`INIT_SYNC` is a process when all existing table data is scanned and pushed into Kafka destination topic. Usually this happens only once after source task for specific table is started for the thirst time. But it can be repeated in case of unexpected issues. For e.g. if source connector was down for long period of time and it is possible that it has missed some of the change events from the table stream (as data is stored only for 24 hours). 

### 3. "SYNC"

Once `INIT_SYNC` is finished source task switches into DynamoDB Streams consumer state. There all changes happening to the source table are represented in this stream and copied over to the Kafka's destination topic. Consumers of this topic can recreate full state of the source table at any given time.

# How does it work

This plugin depends on Kafka Connect framework for most tasks related to Kafka and uses Kinesis Client Library(KCL) + DynamoDB Streams Adapter libraries for DynamoDB Streams consumption. 

Read the following articles to familiarize yourself with them:
* [Connector Developer Guide](https://docs.confluent.io/current/connect/devguide.html)
* [KCL](https://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-kcl.html)
* [KCL DynamoDB adapter](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.KCLAdapter.html)

At it's core this plugin starts one Kafka Connect task for each table it syncs. And each task starts a dedicated KCL(Kinesis Consumer Library) worker to read data from the stream. 

## State tracking 

Connector tracks it's state at all stages and is able to continue there it stopped after restart. But, state and progress **tracking happens at regular intervals** and not after each processed event. Meaning that there can and **will be event duplicates in destination topic**!

Since we are using two different frameworks/libraries together there are two different ways how each of them stores state:
* Kafka connect leverages dedicated `state` topics there connector tasks can push offsets(state) for each partition they are consuming. This plugin has no support for source table "partitions" and only one task is allowed to consume one table at a time therefor it uses table name as partition key and leverage `offsets` dictionary to store tasks state and progress of that state.
* KCL library uses separate dedicated DynamoDB table for each DynamoDB Stream it tracks to remember it's own progress. It is used only to track which messages have been consumed already. Since we can only say that message has been consumed once it's delivered to Kafka special synchronization logic is implemented in this plugin.
  
> NOTE: KCL library separate `state` table in DynamoDB for each stream it tracks! This table is created automatically if it doesn't exist.

### `DISCOVERY` and task configuration

Plugin uses resource group api to receive a list of DynamoDB tables which have ingestion TAG defined. Then it iterates over this list and checks if stack TAG is set and streams are actually enabled. For each table which meats all requirements separate dedicated Kafka Connect task is started.

Same `discovery` phase is executed on start and after every 60 seconds(default config value). Each started task can be in one of the following states.


 `INIT_SYNC` state 

During `INIT_SYNC` phase all records from source table are scanned in batches. After each batch `EXCLUSIVE_START_KEY` is set as offset data with each record. In case of restart `INIT_SYNC` will continues from this start key. Once all records have been read `INIT_SYNC` is marked as finished in offsets and `SYNC` mode starts. 

> NOTE: On start `INIT_SYNC` is delayed by configurable amount of time (by default 60s). This is done to give connect cluster time to settle down after restart and helps to lower amount of duplicates because of connect task rebalances.

### `SYNC` state

After `INIT_SYNC` plugin starts reading messages from DynamoDB Stream. Thirst thing it makes sure is to drop all events which happened before `INIT_SYNC` was started (except for those created during last hour before `INIT_SYNC`). This is done to prevent unnecessary duplicate events(since we already have latest state) and to advance KCL reader into `save zone`. 

Events are considered to be in `save zone` if they there create no earlier then -20 hours before `now`. Otherwise plugin has no way to validate that it hasn't skipped some of the events and it has to initiate `INIT_SYNC`!

> NOTE: DynamoDB Streams store data for 24hours only
