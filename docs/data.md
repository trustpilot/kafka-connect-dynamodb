
# Topic messages structure

* `key` - contains all defined DynamoDB table keys. 
* `value` - contains full DynamoDB document serialised as DynamoDB Json string together with additional metadata.

```json
[
  {
    "topic": "dynamodb-datalake-testdynamoconnector7-staging",
    "key": {
      "id": "5028"
    },
    "value": {
      "version": "0.1",
      "document": "{\"attribute-ns\":{\"ns\":[\"3\",\"1\"]},\"attribute-bool\":{\"bool\":true},\"attribute-ss\":{\"ss\":[\"1aa\",\"2aa\"]},\"attribute-1\":{\"s\":\"test185347\"},\"attribute-m\":{\"m\":{\"key1\":{\"s\":\"MTYPE\"}}},\"id\":{\"s\":\"5028\"},\"attribute-null\":{\"null\":true}}",
      "source": {
        "version": {
          "string": "0.1"
        },
        "table_name": "datalake-testdynamoconnector7-staging",
        "init_sync": "RUNNING",
        "init_sync_start": 1557387726273,
        "init_sync_end": null
      },
      "op": {
        "string": "r"
      },
      "ts_ms": {
        "long": 1557387726273
      }
    },
    "partition": 0,
    "offset": 1
  }
]
```

`op` - Operation field has 4 possible values:
  * `r` - record was read from table during `INIT_SYNC`
  * `c` - new record created
  * `u` - existing record updated
  * `d` - existing record deleted

`init_sync_start` - is set when `INIT_SYNC` starts and will retain same value not only for `INIT_SYNC` records but for all following events as well. Untill next `INIT_SYNC` events happens.

## Delete records

Note that when connector detects delete event, it creates two event messages: 
 * a delete event message with `op` type `d` and empty `document` field.
 * a tombstone message contains same key as the delete message, but the entire message value is null.
    * Kafka’s log compaction utilizes this to know that it can delete all messages for this key.

### Tombstone message sample
```json
[
  {
    "topic": "dynamodb-datalake-testdynamoconnector7-staging",
    "key": {
      "id": "5028"
    },
    "value": null,
    "partition": 0,
    "offset": 1
  }
]
```