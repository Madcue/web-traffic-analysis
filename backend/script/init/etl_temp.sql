DROP TABLE IF EXISTS session_avro_tmp;

--TBLPROPERTIES  STORED AS存储在key value 路径'avro.schema.url'='hdfs://master:9999/user/hadoop-twq/traffic-analysis/avro/Session.avsc'
CREATE EXTERNAL TABLE IF NOT EXISTS session_avro_tmp
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
    STORED AS
    INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
    LOCATION "hdfs://master:9999/user/hadoop-twq/traffic-analysis/web/session/year=2018/month=201806/day=20180615"
    TBLPROPERTIES(
    'avro.schema.url'='hdfs://master:9999/user/hadoop-twq/traffic-analysis/avro/Session.avsc'
    );

DROP TABLE IF EXISTS pageview_avro_tmp;
CREATE EXTERNAL TABLE IF NOT EXISTS pageview_avro_tmp
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
    STORED AS
    INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
    LOCATION "hdfs://master:9999/user/hadoop-twq/traffic-analysis/web/pageview/year=2018/month=201806/day=20180615"
    TBLPROPERTIES (
        'avro.schema.url'='hdfs://master:9999/user/hadoop-twq/traffic-analysis/avro/PageView.avsc'
    );

DROP TABLE IF EXISTS mouseclick_avro_tmp;
CREATE EXTERNAL TABLE IF NOT EXISTS mouseclick_avro_tmp
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
    STORED AS
    INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
    LOCATION "hdfs://master:9999/user/hadoop-twq/traffic-analysis/web/mouseclick/year=2018/month=201806/day=20180615"
    TBLPROPERTIES (
        'avro.schema.url'='hdfs://master:9999/user/hadoop-twq/traffic-analysis/avro/MouseClick.avsc'
    );

DROP TABLE IF EXISTS heartbeat_avro_tmp;
CREATE EXTERNAL TABLE IF NOT EXISTS heartbeat_avro_tmp
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
    STORED AS
    INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
    LOCATION "hdfs://master:9999/user/hadoop-twq/traffic-analysis/web/heartbeat/year=2018/month=201806/day=20180615"
    TBLPROPERTIES (
        'avro.schema.url'='hdfs://master:9999/user/hadoop-twq/traffic-analysis/avro/Heartbeat.avsc'
    );

DROP TABLE IF EXISTS conversion_avro_tmp;
CREATE EXTERNAL TABLE IF NOT EXISTS conversion_avro_tmp
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
    STORED AS
    INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
    LOCATION "hdfs://master:9999/user/hadoop-twq/traffic-analysis/web/conversion/year=2018/month=201806/day=20180615"
    TBLPROPERTIES (
        'avro.schema.url'='hdfs://master:9999/user/hadoop-twq/traffic-analysis/avro/Conversion.avsc'
    );