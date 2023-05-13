package com.lc.common;

/**
 * ClassName:KafkaConfig
 * Package:com.lc.common
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-04-25-16:01
 */
public class MyConfiguration {

    //kafaka配置常量
    public static final String KAFKA_BOOTSTRAP_SERVERS = "hadoop102:9092";

    public static final String HBASE_SCHEMA = "GMALL221109_REALTIME";

//    public static final String database = "gmall";
//
//    public static final String table = "gmall";
    //mysql配置常量


    public static final String MYSQL_HOSTNAME = "hadoop102";

    public static final String SCHEMA_QUERY = "SELECT table_name,column_name,data_type,column_comment,column_key \n" +
            "FROM information_schema.COLUMNS \n" +
            "WHERE TABLE_SCHEMA = ? and table_name = ?";

    public static final String EARLIEST_OFFSET = "earliest-offset";
    public static final String LATEST_OFFSET = "latest-offset";
    public static final String GROUP_OFFSETS = "group-offsets";
    public static final String TIMESTAMP = "timestamp";


    public static final String MAXWEL_SOURCE_COLUMN = " `database` STRING, \n" +
            " `table` STRING, \n" +
            " `type` STRING, \n" +
            " `data` MAP<STRING,STRING>, \n" +
            " `old` MAP<STRING,STRING>, \n" +
            " `ts` BIGINT";



    public static final String KAFKA_SOURCE_WITH = "CREATE TABLE %s " + //表名
            "%s\n" +
            ") WITH (\n" +
            "'connector' = 'kafka',\n" +
            "'topic' = '%s',\n" +
            "'properties.bootstrap.servers' = '"+MyConfiguration.KAFKA_BOOTSTRAP_SERVERS+"',\n" +
            "'properties.group.id' = '%s',\n"+
            "'format' = 'json',\n" +
            "'scan.startup.mode' = '%s',\n"+
            "'value.fields-include' = 'EXCEPT_KEY'";


    public static final String KAFKA_SINK_WITH = "CREATE TABLE %s " + //表名
            "%s\n" +
            ") WITH (\n" +
            "'connector' = 'kafka',\n" +
            "'topic' = '%s',\n" +
            "'properties.bootstrap.servers' = '"+MyConfiguration.KAFKA_BOOTSTRAP_SERVERS+"',\n" +
            "'format' = 'json',\n" +
            "'value.fields-include' = 'EXCEPT_KEY'";


    public static final String UPSERT_KAFKA_SINK_WITH= "CREATE TABLE %s " + //表名
            "%s\n" +
            ") WITH ( \n" +
            "'connector' = 'kafka',\n" +
            "'topic' = '%s',\n" +
            "'properties.bootstrap.servers' = '"+MyConfiguration.KAFKA_BOOTSTRAP_SERVERS+"',\n" +
            "'key.format' = 'json',\n" +
            "'value.format' = 'json',\n" +
            "'value.fields-include' = 'EXCEPT_KEY'";

    public static final String FILTER_TABLE = "SELECT \n" +
            "%s\n" +
            "FROM %s\n" +
            "WHERE DATABASE = "+MyConfiguration.SCHEMA+"\n" +
            "AND TABLE = %s\n" +
            "AND TYPE = %s \n";

    public static final String WHERE = "WHERE DATABASE = "+MyConfiguration.SCHEMA+"\n" +
            "   AND TABLE = %s\n" +
            "   AND TYPE = %s \n" +
            "   %s";


    public static final String JOIN = "join %s %s \n" +
            "on %s";

    public static final String SCHEMA = "gmall";

    public static final String BACKEND_TABLE = "autoddl.backend_table";



}
