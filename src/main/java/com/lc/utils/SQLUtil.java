package com.lc.utils;

/**
 * ClassName:SQLUtil
 * Package:com.lc.utils
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-04-27-20:19
 */
public class SQLUtil {

    public static String getBaseDicDDL(){
        return "create table `base_dic`(\n" +
                "`dic_code` string,\n" +
                "`dic_name` string,\n" +
                "`parent_code` string,\n" +
                "`create_time` timestamp,\n" +
                "`operate_time` timestamp,\n" +
                "primary key(`dic_code`) not enforced\n" +
                ") WITH (\n" +
                "'connector' = 'jdbc',\n" +
                        "'url' = 'jdbc:mysql://hadoop102:3306/gmall',\n" +
                        "'table-name' = 'base_dic',\n" +
                        "'lookup.cache.max-rows' = '10',\n" +
                        "'lookup.cache.ttl' = '1 hour',\n" +
                        "'username' = 'root',\n" +
                        "'password' = '000000',\n" +
                        "'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                        ")";
    }
}
