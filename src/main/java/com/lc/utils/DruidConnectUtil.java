package com.lc.utils;

import cn.hutool.db.ds.DSFactory;
import cn.hutool.db.ds.druid.DruidDSFactory;
import org.apache.commons.dbutils.QueryRunner;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * ClassName:ConnectUtil
 * Package:com.lc.utils
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-04-27-0:24
 */
public class DruidConnectUtil {


    static {
        //定义druid连接方式
        DSFactory.setCurrentDSFactory(new DruidDSFactory());
    }

    //PHoenix连接
    public static Connection getPhoenixConnection()  {

        try {
            return DSFactory.get("phoenix").getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("获取连接失败 "+e);
        }

    }

    //PHoenix连接
    public static DataSource getPhoenixDataSource()  {

        try {
            return DSFactory.get("phoenix");
        } catch (Exception e) {
            throw new RuntimeException("获取连接失败 "+e);
        }

    }

    public static MyDBHelper getPhoenixDBHelper()  {
        try {
          return new MyDBHelper(new QueryRunner(DSFactory.get("phoenix")));
        } catch (Exception e) {
            throw new RuntimeException("获取连接失败 "+e);
        }
    }

    //mysql连接
    public static Connection getMysqlConnection()  {

        try {
            return DSFactory.get("mysql").getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("获取连接失败 "+e);
        }

    }

    public static DataSource getMysqlDataSource()  {
        try {
            return DSFactory.get("mysql");
        } catch (Exception e) {
            throw new RuntimeException("获取连接失败 "+e);
        }

    }

    public static MyDBHelper getMysqlDBHelper()  {
        try {
            return new MyDBHelper(new QueryRunner(DSFactory.get("mysql")));
        } catch (Exception e) {
            throw new RuntimeException("获取连接失败 "+e);
        }
    }


    //Clickhouse连接
    public static Connection getClickhouseConnection()  {

        try {
            return DSFactory.get("clickhouse").getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("获取连接失败 "+e);
        }
    }

    public static DataSource getClickhouseDataSource()  {

        try {
            return DSFactory.get("clickhouse");
        } catch (Exception e) {
            throw new RuntimeException("获取连接失败 "+e);
        }

    }

    public static MyDBHelper getClickhouseDBHelper()  {
        try {
            return new MyDBHelper(new QueryRunner(DSFactory.get("clickhouse")));
        } catch (Exception e) {
            throw new RuntimeException("获取连接失败 "+e);
        }
    }



    //关闭连接
    public static void close(ResultSet resultSet, PreparedStatement preparedStatement, Connection connection) {

        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }


}
