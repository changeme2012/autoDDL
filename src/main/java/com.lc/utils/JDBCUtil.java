package com.lc.utils;

import cn.hutool.json.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName:JDBCUtil
 * Package:com.lc.utils
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-04-27-11:27
 */
public class JDBCUtil {

    //多行查询
    public static <T> List<T> queryList(Connection connection,String sql , Class<T> tClass , Boolean underScoreToCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {

        ArrayList<T> arrayList = new ArrayList<>();

        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        ResultSet resultSet = preparedStatement.executeQuery();

        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();

        T t = tClass.newInstance();

        while (resultSet.next()){

            for (int i = 1; i < columnCount +1; i++) {

                String columnName = metaData.getColumnName(i);

                String value = resultSet.getString(columnName);


                if (underScoreToCamel){

                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_HYPHEN, columnName);
                }

                BeanUtils.setProperty(t,columnName,value);

            }

            arrayList.add(t);

        }

        resultSet.close();
        preparedStatement.close();


        return arrayList;



    }
    public static void main(String[] args) throws Exception {

        Connection connection = DruidConnectUtil.getPhoenixConnection();

        List<JSONObject> jsonObjects = queryList(connection,
                "select * from student",
                JSONObject.class,
                false);

        for (JSONObject jsonObject : jsonObjects) {
            System.out.println(jsonObject);
        }

        connection.close();

    }


}
