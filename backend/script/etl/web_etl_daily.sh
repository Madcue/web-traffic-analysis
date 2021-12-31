#!/usr/bin/env bash

yesterday=$1

if [ -z "$yesterday" ];then
  yesterday=`date --d="1 days ago" +%Y%m%d`
  fi
year=${yesterday:0:4}
month=${yesterday:0:6}
day=$yesterday

rm tranform_${day}.sql

for table_name in "Session" "PageView" "MouseClick" "Heartbeat" "Conversion"
do
  lower_table_name=`echo $table_name | tr 'A-Z' 'a-z'`
  echo "starting  $lower_table_name transformed"
  drop_tmp_table_sql="DROP TABLE IF EXISTS web_tmp.${lower_table_name}_avro_tmp;"
  create_tmp_table_sql="CREATE EXTERNAL TABLE IF NOT EXISTS web_tmp.${lower_table_name}_avro_tmp
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
    STORED AS
    INPUTFORMAT  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
    LOCATION 'hdfs://master-dev:9999/user/hadoop-twq/traffic-analysis/web/${lower_table_name}/year=${year}/month=${month}/day=${day}'
    TBLPROPERTIES(
    'avro.schema.url'='hdfs://master-dev:9999/user/hadoop-twq/traffic-analysis/avro/${table_name}.avsc'
    );"
  hadoop fs -rm -r /user/hive/warehouse/web.db/${lower_table_name}/year=${year}/month=${month}/day=${day}
  # has a bug : https://issues.apache.org/jira/browse/HIVE-16958
  insert_overwrite_target_table_sql="set hive.merge.mapfiles=false;INSERT OVERWRITE TABLE web.${lower_table_name}
  PARTITION(year=$((year)), month=$((month)), day=$((day))) SELECT tmp.* FROM web_tmp.${lower_table_name}_avro_tmp tmp;"
  echo ${drop_tmp_table_sql} >> transform_${day}.sql
  echo ${create_tmp_table_sql} >> transform_${day}.sql
  echo ${insert_overwrite_target_table_sql} >> transform_${day}.sql
done

echo "exit;" >> transform_${day}.sql
hive -f transform_${day}.sql