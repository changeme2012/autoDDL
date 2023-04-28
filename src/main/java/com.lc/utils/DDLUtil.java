package com.lc.utils;

import com.lc.bean.tableBean;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName:DDLUtil
 * Package:com.lc.utils
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-04-28-17:04
 */
public class DDLUtil {



    public static void  getResultSet(Connection connection, String database, List<String> tableList) throws SQLException {


        String SCHEMA_QUERY = "SELECT table_name,column_name,data_type,column_comment,column_key \n" +
                "FROM information_schema.COLUMNS \n" +
                "WHERE TABLE_SCHEMA = ? and table_name = ?";

        ResultSet resultSet =  null;

        PreparedStatement preparedStatement = null;

        ArrayList<ResultSet> resultSets = new ArrayList<>();

         List<tableBean> tableBeanList;


        for (String tableName : tableList) {

            tableBeanList = QueryUtil.queryList(connection, SCHEMA_QUERY, tableBean.class, true, database, tableName);

        }

        getSourceDDL(resultSets);

    }

    public static List<String> getSourceDDL(List<ResultSet> resultSetList){



        return null;

    }




}
