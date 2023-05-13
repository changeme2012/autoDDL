package com.lc.utils;

import cn.hutool.core.collection.CollUtil;
import com.lc.bean.tableBean;
import com.lc.common.MyConfiguration;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * ClassName:DDLUtil
 * Package:com.lc.utils
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-04-28-17:04
 */
public class DDLUtil {

    public static void main(String[] args) throws SQLException {




     String filter =  "and type = 'insert' " ;

        divideTable("topic_db","gmall","order_detail_activity",filter);

//        getResultSet("gmall","order_detail_activity");
    }


    private static final String KAFKA_SERVERS = "hadoop102:9092";


    private static MyDBHelper dbHelper;

    static {
        dbHelper = DruidConnectUtil.getMysqlDBHelper();
    }



    public static List<tableBean>  getResultSet( String database,String table) throws SQLException {

        List<tableBean> tableBeanList = dbHelper.queryList(MyConfiguration.SCHEMA_QUERY, tableBean.class, true,database,table);

        return tableBeanList;

    }

    public static String  divideTable( String topic,String database, String table,String filterSql) throws SQLException {

        List<tableBean> tableBeanList = dbHelper.queryList(MyConfiguration.SCHEMA_QUERY, tableBean.class, true,database,table);

        String sourceDML = getColumn(tableBeanList, topic, database, table);

        return sourceDML;
    }

    public static String lookUPJoin(List<tableBean> resultSetList ,String sinkTableName ,String topic){
        ArrayList<String> sqlList = new ArrayList<>();

        StringBuffer sql = new StringBuffer("select  ");

        String joinWord = "";

        for (tableBean tableBean : resultSetList) {
            if ("来源编号".equals(tableBean.getColumnComment())){
                joinWord = tableBean.getColumnName();
            }
        }

        String collect = resultSetList.stream().map(tableBean -> {

            return "`" + tableBean.getColumnName() +"`";
        }).collect(Collectors.joining(",\n"));

        sql.append(sinkTableName + " (\n")

                .append(collect);

        sqlList.add(sql.toString());

        String tableSourceSql = getTableSinkSql(topic, sqlList);

        return tableSourceSql;

    }

    public static String getQureyDDL(List<tableBean> resultSetList ){

        ArrayList<String> sqlList = new ArrayList<>();

        StringBuffer sql = new StringBuffer("select\n");

        AtomicReference<String> pri = new AtomicReference<>("");

        String collect = resultSetList.stream().map(tableBean -> {

            if ("PRI".equals(tableBean.getColumnKey())){
                pri.set(tableBean.getColumnName());
            }
            return "`" + tableBean.getColumnName()  ;


        }).collect(Collectors.joining(",\n"));

        sql.append(collect)
                .append(",\n")
//                .append(  " PRIMARY KEY ( " + pri.get() + " ) NOT ENFORCED")
                .append("`pt`\n");

        sqlList.add(sql.toString());

        String join = CollUtil.join(sqlList, "");

        return join;

    }

    public static String getSourceDML(List<tableBean> resultSetList ,String topic,String database ,String table,String filterSql){

        ArrayList<String> sqlList = new ArrayList<>();

        StringBuffer sql = new StringBuffer("select \n");

        String collect = resultSetList.stream().map(tableBean -> {

            return "\tdata['" + tableBean.getColumnName()  + "'] " + tableBean.getColumnName();

        }).collect(Collectors.joining(",\n"));

        sql.append(collect).append(",\n").append("`pt`\n");

        sqlList.add(sql.toString());

        String tableSourceSql = getDMLSql(topic, sqlList,database,table ,filterSql);

        return tableSourceSql;

    }

    public static String getColumn(List<tableBean> resultSetList ,String topic,String database ,String table){

        ArrayList<String> sqlList = new ArrayList<>();

        StringBuffer sql = new StringBuffer();

        String collect = resultSetList.stream().map(tableBean -> {

            return "\tdata['" + tableBean.getColumnName()  + "'] " + tableBean.getColumnName();

        }).collect(Collectors.joining(",\n"));

        sql.append(collect).append(",\n\t`pt`\n");

        sqlList.add(sql.toString());

       return String.join("",sqlList);

    }

    public static String getJoinSql(String topic,List<String> sqlList,String lookUpTable ,String table){

        String sourceDDL =
                "from `"+table+"` as t1\n" +
                        "join "+lookUpTable+" FOR SYSTEM_TIME AS OF t1.pt AS t2`\n" +
                        "on t1.source_type = t2.dic_code`\n";

        StringBuffer sqlSourceDDL = new StringBuffer();

        for (String sql : sqlList) {
            sqlSourceDDL.append(sql + sourceDDL);
        }

        return sqlSourceDDL.toString();

    }


    public static String getDMLSql(String topic,List<String> sqlList,String database ,String table,String filterSql){

        String sourceDDL =
                "from `"+topic+"`\n" +
                "where `database` = '"+database+"'\n" +
                "and `table` = '"+table+"'\n" + filterSql;



        StringBuffer sqlSourceDDL = new StringBuffer();

        for (String sql : sqlList) {
            sqlSourceDDL.append(sql + sourceDDL);
        }

        return sqlSourceDDL.toString();

    }


    public static String getTableSinkSql(String topic,List<String> sqlList){

        String sinkeDDL = " ) WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '"+topic+"',\n" +
                "  'properties.bootstrap.servers' = '"+KAFKA_SERVERS+"',\n" +
                "  'format' = 'json'\n" +
                ")";


        StringBuffer sqlSinkDDL = new StringBuffer();

        for (String sql : sqlList) {
            sqlSinkDDL.append(sql + sinkeDDL);
        }

        return sqlSinkDDL.toString();

    }




}
