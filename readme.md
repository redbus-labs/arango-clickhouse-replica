# Replica

Replica is a tool to replicate the data from ArangoDB to Clickhouse in real-time.

[![architecture](https://lh5.googleusercontent.com/1ogFwFc_I6MgooB-IWOxge13bj5-FPEJQkvuvfwpBknMTpp-YQAREKI2GaJYAWT6a24ftVel_FKCCR6JA9m7W7eh6c4GLo-uAnIbJarYsYpec_et-X-pPaF4LkNeP05PDxAXVm4f "architecture")](https://lh5.googleusercontent.com/1ogFwFc_I6MgooB-IWOxge13bj5-FPEJQkvuvfwpBknMTpp-YQAREKI2GaJYAWT6a24ftVel_FKCCR6JA9m7W7eh6c4GLo-uAnIbJarYsYpec_et-X-pPaF4LkNeP05PDxAXVm4f "architecture")

### Features
- Realtime replication using arango write-ahead logs
- Supports Insert, update and delete events
- Flexible data transformation
- Consumer threads management and monitoring

### Requirements
- ArangoDB 3.4+
- Clickhouse 20+
- Python 3.7
- Kafka 2.8
- Redis 3.2
- PM2

### Installation
```shell
git clone https://github.com/redbus-labs/arango-clickhouse-replica.git
virtualenv venv
source venv/bin/activate
python3 setup.py install
```

### Configuration

#### Settings

Configure the required details in the settings.yaml.

Example:

```yaml
arango:
  host: arango-service.com
  port: 8529
  username: username
  password: password
  db: db_name

wal:
  username: root_user
  password: root_password

clickhouse:
  host: clickhouse-service.com
  port: 9000
  database: db_name
  user: username
  password: password

kafka:
  host: kafka-service.com
  port: 9092

redis:
  host: redis-service.com
  port: 6379
  db: 0

producer:
  # provide the list of collections to sync from arango
  sync:
    - collection1
    - collection2

alert:
  # optional: enable this flag and provide the smtp configuration to receive the email alerts
  enable: false
```

#### Table settings:

Place all the table settings inside the table directory. Clickhouse table definitions are required to create the table on the fly.

Specify the table schema which is responsible for the data typecasting and the data validation. Since the arango tables are schema-less, these schema definitions will help to transform the arango document into clickhouse compatible records.

[Buffer table](https://clickhouse.tech/docs/en/engines/table-engines/special/buffer/ "Buffer table") can be enabled if required to handle write-heavy tables.

Every table will have a separate topic in Kafka, and we can optionally specify any custom topic configurations if required. 

Custom type casting functions are supported, define all the custom functions in the transform.py.

Note:
1. Table engine should be [ReplacingMergeTree](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/replacingmergetree/ "ReplacingMergeTree") (essential to handle data updates).
2. Mark the _key column from arango as the primary key in the schema, and it should be part of the primary key in the Clickhouse table definition.
3. _ver and _deleted columns are mandatory, which helps to keep track of update and delete operations.

Example:

```yaml
---
table_name: Test

table: |
  CREATE TABLE Test
  (
      `Id`             Int64,
      `Name`           String,
      `Email`          String,
      `Answers`        Array(String),
      `SubmittedOn`    Nullable(DateTime),
      `_ver`           UInt64,
      `_rev`           String,
      `_deleted`       UInt8
  ) ENGINE = ReplacingMergeTree(_ver)
        PRIMARY KEY (Id, Name)
        ORDER BY (Id, Name)
        SETTINGS index_granularity = 256

schema:
  properties:
    Id:
      type: int
      ref: _key # arango column name
    Name:
      type: str
      ref: name
      required: true
    Email:
      type: str
      ref: email
    Answers:
      type: to_array # custom transform function
      default: [ ]
    SubmittedOn:
      type: from_datetime
      ref: submitted_on
    _rev:
      type: str
      default: ''
    _ver:
      type: int
      default: 1
    _deleted:
      type: int
      default: 0
  primary_key: Id

# optional
buffer:
  num_layers: 1
  min_time: 15
  max_time: 30
  min_rows: 1000
  max_rows: 2000
  min_bytes: 10000000
  max_bytes: 20000000

# optional
topic_config:
  cleanup.policy: compact,delete
  retention.ms: 172800000
  delete.retention.ms: 1800000
  min.compaction.lag.ms: 1800000
  max.compaction.lag.ms: 7200000
  min.cleanable.dirty.ratio: 0.3
  segment.ms: 1800000
```

#### Arango

Increase the arango wal file timeout
```markdown
wal-file-timeout=3600
wal-directory=/var/lib/arangodb3/wal
```


### Usage

```shell
python3 replicate.py -c # sync all the configured tables
python3 replicate.py -t table_name1,table_name2 # sync only the specified tables
```


### Monitoring

```shell
python3 taskmanager.py -info
```