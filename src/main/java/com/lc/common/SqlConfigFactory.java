package com.lc.common;

import lombok.Data;

import java.util.HashMap;

/**
 * ClassName:SqlConfigFactory
 * Package:com.lc.common
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-05-11-11:09
 */

@Data

public class SqlConfigFactory {
    String create;
    String exclude;
    String include;
    HashMap<String,String[]> columnExp = new HashMap<>();
    HashMap<String,String[]> function = new HashMap<>();
    String filterTable;
    String where;
    HashMap<String,String>  join = new HashMap<>();
    String lookpuJoin;
    String database;
    String table;
    String topic;
    String groupId;
    //表别名，在创建表时
    String saveTableAlias;
    String scanMode;
    String maxwelSourceColumn;
    String kafkaSourceWith;
    String kafkaSinkWith;
    String upsertKafkaSinkWith;
    String select;


    public static final String QUERY = "SELECT \n" +
            "%s\n" +
            "FROM %s ";

    public static final String WHERE = "WHERE DATABASE = "+MyConfiguration.SCHEMA+"\n" +
            "   AND TABLE = %s\n" +
            "   AND TYPE = %s \n" +
            "   %s";

    public static final String MAXWEL_SOURCE_COLUMN = "" +
            "   `database` STRING, \n" +
            "   `table` STRING, \n" +
            "   `type` STRING, \n" +
            "   `data` MAP<STRING,STRING>, \n" +
            "   `old` MAP<STRING,STRING>, \n" +
            "   `ts` BIGINT";


    public static final String KAFKA_SOURCE_WITH = "CREATE TABLE %s (\n" + //表名
            "%s\n" +
            ") WITH (\n" +
            "   'connector' = 'kafka',\n" +
            "   'topic' = '%s',\n" +
            "   'properties.bootstrap.servers' = '"+MyConfiguration.KAFKA_BOOTSTRAP_SERVERS+"',\n" +
            "   'properties.group.id' = '%s',\n"+
            "   'format' = 'json',\n" +
            "   'scan.startup.mode' = '%s',\n"+
            "   'value.fields-include' = 'EXCEPT_KEY'";


    public static final String KAFKA_SINK_WITH = "CREATE TABLE %s (\n" + //表名
            "%s\n" +
            ") WITH (\n" +
            "   'connector' = 'kafka',\n" +
            "   'topic' = '%s',\n" +
            "   'properties.bootstrap.servers' = '"+MyConfiguration.KAFKA_BOOTSTRAP_SERVERS+"',\n" +
            "   'format' = 'json',\n" +
            "   'value.fields-include' = 'EXCEPT_KEY'";


    public static final String UPSERT_KAFKA_SINK_WITH= "CREATE TABLE %s (\n" + //表名
            "%s\n" +
            ") WITH ( \n" +
            "   'connector' = 'kafka',\n" +
            "   'topic' = '%s',\n" +
            "   'properties.bootstrap.servers' = '"+MyConfiguration.KAFKA_BOOTSTRAP_SERVERS+"',\n" +
            "   'key.format' = 'json',\n" +
            "   'value.format' = 'json',\n" +
            "   'value.fields-include' = 'EXCEPT_KEY'";


    public SqlConfigFactory create(String createTable) {
        this.create = exclude;
        return this;
    }

    public SqlConfigFactory exclude(String exclude) {
        this.exclude = exclude;
        return this;
    }

    public SqlConfigFactory include(String include) {
        this.include = include;
        return this;
    }

    public SqlConfigFactory columnExp(String columnExp,String...function) {
        this.columnExp.put(columnExp,function);
        return this;
    }

    public SqlConfigFactory function(String column,String...function) {
        this.function.put(column,function);
        return this;
    }

    public SqlConfigFactory where(String where) {
        this.where = where;
        return this;
    }

    public SqlConfigFactory join(String joinTableName,String onField) {
        this.join.put(joinTableName,onField);
        return this;
    }

    public SqlConfigFactory lookupJoin(String lookupJoin) {
        this.lookpuJoin = lookupJoin;
        return this;
    }

    public SqlConfigFactory database(String database) {
        this.database = database;
        return this;
    }

    public SqlConfigFactory table(String table) {
        this.table = table;
        return this;
    }

    public SqlConfigFactory saveTableAlias(String saveTableAlias) {
        this.saveTableAlias = saveTableAlias;
        return this;
    }

    public SqlConfigFactory topic(String topic) {
        this.topic = topic;
        return this;
    }

    public SqlConfigFactory groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public SqlConfigFactory scanMode(String scanMode) {
        this.scanMode = scanMode;
        return this;
    }


    public SqlConfigFactory filterTable(String fromTableName,String filterTableName,String type) {
        this.filterTable = String.format(MyConfiguration.FILTER_TABLE,"%s",fromTableName,filterTableName,type) ;
        return this;
    }


    public SqlConfigFactory select(String fromTableName) {
//        this.select = String.format(QUERY,"%s",fromTableName) ;
        this.select = fromTableName;
        return this;
    }



    public SqlConfigFactory maxwelSourceColumn() {
        this.maxwelSourceColumn = MAXWEL_SOURCE_COLUMN;
        return this;
    }

    public SqlConfigFactory kafkaSourceWith(String createTableName) {
        if (scanMode == null){
            this.kafkaSourceWith = String.format(KAFKA_SOURCE_WITH,this.table,"%s",this.topic, this.groupId, MyConfiguration.EARLIEST_OFFSET);
            return this;
        }else {
            this.kafkaSourceWith = String.format(KAFKA_SOURCE_WITH, this.table,"%s",this.topic, this.groupId, this.scanMode);
            return this;
        }
    }

    public SqlConfigFactory kafkaSinkWith(String createTableName) {
        if (scanMode == null){
            this.kafkaSinkWith = String.format(KAFKA_SINK_WITH,this.table,"%s", this.topic);
            return this;
        }else {
            this.kafkaSourceWith = String.format(KAFKA_SINK_WITH,this.table,"%s", this.topic);
            return this;
        }
    }

    public SqlConfigFactory upsertKafkaSinkWith(String createTableName) {
        if (scanMode == null){
            this.upsertKafkaSinkWith = String.format(UPSERT_KAFKA_SINK_WITH, this.table,"%s",this.topic);
            return this;
        }else {
            this.upsertKafkaSinkWith = String.format(UPSERT_KAFKA_SINK_WITH,this.table,"%s", this.topic);
            return this;
        }
    }




}
