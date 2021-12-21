
-- SET sql-client.execution.result-mode=tableau;
-- SET sql-client.verbose=true;
-- set table.exec.source.cdc-events-duplicate=true;

-- 约1000万条数据
create table user_behavior_small_ods
(
    user_id       Integer,
    item_id       Integer,
    category_id   Integer,
    behavior_type String,
    time_stamp    BIGINT
) WITH(
    'connector' = 'filesystem',
    'path' = '/opt/flink-1.13.3/user_behavior-1KW.log',
    'format' = 'csv',
    'csv.field-delimiter' = ','
);

create table user_behavior_small_kafka
(
    user_id       Integer,
    item_id       Integer,
    category_id   Integer,
    behavior_type String,
    time_stamp    BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'user_behavior_small',
    'properties.bootstrap.servers' = '172.16.83.252:9092',
    'format' = 'debezium-json',
    'debezium-json.ignore-parse-errors' = 'true'
);

insert into user_behavior_small_kafka
select user_id, item_id, category_id, behavior_type, time_stamp
from user_behavior_small_ods;


create table user_behavior_small_kafka_source
(
    `offset`      BIGINT NOT NULL METADATA FROM 'offset',
    user_id       Integer,
    item_id       Integer,
    category_id   Integer,
    behavior_type String,
    time_stamp    BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'user_behavior_small',
    'properties.bootstrap.servers' = '172.16.83.252:9092',
    'properties.group.id' = 'user_behavior_small',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json',
    'debezium-json.ignore-parse-errors' = 'true'
);

create table user_behavior_small_sink_jdbc
(
    id              BIGINT,
    user_id         Integer,
    item_id         Integer,
    category_id     Integer,
    behavior_type   String,
    create_time     TIMESTAMP,
    update_time     TIMESTAMP,
    PRIMARY KEY (id) NOT ENFORCED
) WITH(
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://localhost:3306/flink',
    'table-name' = 'user_behavior_small',
    'username' = 'root',
    'password' = 'root'
);

insert into user_behavior_small_sink_jdbc
select `offset`                                               as id,
       user_id, item_id, category_id, behavior_type,
       cast(FROM_UNIXTIME(time_stamp) as TIMESTAMP(0))      as create_time,
       cast(FROM_UNIXTIME(time_stamp) as TIMESTAMP(0))      as update_time
from user_behavior_small_kafka_source;