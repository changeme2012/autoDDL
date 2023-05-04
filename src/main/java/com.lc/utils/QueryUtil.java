package com.lc.utils;

import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.GenerousBeanProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * ClassName:BasicDAO
 * Package:com.lc.userprofile.dao
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023/3/30/030-15:35
 */
public class QueryUtil {

    private static  QueryRunner runner =  new QueryRunner();

    //返回多个对象(即查询的结果是多行), 针对任意表

    public static  <T> List<T> queryList(Connection connection, String sql, Class<T> clazz ,Boolean underScoreToCamel, String... parameters) throws SQLException {

        List<T> queryList;

        if (underScoreToCamel) {
            //下划线转小驼峰
           queryList = runner.query(connection, sql, new BeanListHandler<T>(clazz, new BasicRowProcessor(new GenerousBeanProcessor())),parameters);
            return queryList;
        }else {
            queryList = runner.query(connection, sql, new BeanListHandler<T>(clazz));
            return queryList;
        }
    }

    //查询单行结果 的通用方法
    public static <T> T queryOneLine(Connection connection, String sql, Class<T> clazz ,Boolean underScoreToCamel) throws SQLException {


            T queryOne = runner.query(connection, sql, new BeanHandler<T>(clazz, new BasicRowProcessor(new GenerousBeanProcessor())));

            return queryOne;

        }
    }


