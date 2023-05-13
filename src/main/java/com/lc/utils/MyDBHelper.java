package com.lc.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.lc.bean.SkipField;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.GenerousBeanProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.*;

/**
 * ClassName:BasicDAO
 * Package:com.lc.userprofile.dao
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023/3/30/030-15:35
 */

public class MyDBHelper {

    private final QueryRunner runner;

    public MyDBHelper(QueryRunner runner) {
        this.runner = runner;
    }

    //返回多个对象(即查询的结果是多行), 针对任意表
    public <T> List<T> queryList(String sql, Class<T> clazz, Boolean underScoreToCamel) throws Exception {

        List<T> queryList;

        ArrayList<T> list = new ArrayList<>();

        T t = clazz.newInstance();
        //如果传参泛型是JSONObject,单独封装
        if (clazz.isAssignableFrom(JSONObject.class)) {
            //把查询结果封装成List<map>,带元数据
            List<Map<String, Object>> query = runner.query(sql, new MapListHandler());
            //遍历List<Map>
            for (Map<String, Object> map : query) {
                Set<Map.Entry<String, Object>> entries = map.entrySet();
                //遍历Map,封装数据
                for (Map.Entry<String, Object> entry : entries) {
                    BeanUtil.setProperty(t, entry.getKey(), entry.getValue());
                }
            }

            list.add(t);

            return list;

        } else {
            if (underScoreToCamel) {
                //下划线转小驼峰
                queryList = runner.query(sql, new BeanListHandler<T>(clazz, new BasicRowProcessor(new GenerousBeanProcessor())));
                return queryList;
            } else {
                queryList = runner.query(sql, new BeanListHandler<T>(clazz));
                return queryList;
            }
        }


    }

    public <T> List<T> queryList(String sql, Class<T> clazz, Boolean underScoreToCamel, String... parameters) throws SQLException {

        List<T> queryList;

        if (underScoreToCamel) {
            //下划线转小驼峰
            queryList = runner.query(sql, new BeanListHandler<T>(clazz, new BasicRowProcessor(new GenerousBeanProcessor())), parameters);
            return queryList;
        } else {
            queryList = runner.query(sql, new BeanListHandler<T>(clazz),parameters);
            return queryList;
        }

    }


    //单条数据写入clickhouse
    public <T> void insert(String tableName, T value) throws SQLException, IllegalAccessException {

        StringBuilder sb = new StringBuilder("REPLACE INTO  " + tableName + " VALUES(");
        // 获取泛型T的所有字段
        Field[] fields = value.getClass().getDeclaredFields();
        //定义存放字段值的数组
        List<Object> params = new ArrayList<>();
        //把泛型的属性字段放入List集
        List<Field> fieldList = new ArrayList<>(Arrays.asList(fields));
        // 过滤掉有跳过注解的字段
        fieldList.removeIf(field -> field.isAnnotationPresent(SkipField.class));

        // 构造 SQL 语句和参数
        for (int i = 0; i < fieldList.size(); i++) {

            //封装字段值
            fieldList.get(i).setAccessible(true);
            params.add(fieldList.get(i).get(value));

            //判断是不是最后一个字段
            sb.append("?");
            if (i != fieldList.size() - 1) {
                sb.append(",");
            }
        }
        sb.append(")");
        // 执行批量插入操作
        runner.update(sb.toString(), params.toArray());
    }

    public <T> void batchInsert(String tableName, List<T> list) throws SQLException, IllegalAccessException {

        StringBuilder sb = new StringBuilder("INSERT INTO " + tableName + " VALUES (");
        List<Object[]> params = new ArrayList<>();

        // 获取泛型T的所有字段
        Field[] fields = list.get(0).getClass().getDeclaredFields();

        List<Field> fieldList = new ArrayList<>(Arrays.asList(fields));
        // 过滤掉有跳过注解的字段
        fieldList.removeIf(field -> field.isAnnotationPresent(SkipField.class));

        for (int i = 0; i < fieldList.size(); i++) {
            sb.append("?");
            if (i != fieldList.size() - 1) {
                sb.append(",");
            }
        }
        sb.append(")");

        for (T t : list) {
            Object[] param = new Object[fieldList.size()];
            for (int i = 0; i < fieldList.size(); i++) {
                Field field = fieldList.get(i);
                field.setAccessible(true);
                param[i] = field.get(t);
            }
            params.add(param);
        }
        // 执行批量插入操作
        runner.batch(sb.toString(), params.toArray(new Object[params.size()][]));
    }




    //查询单行结果 的通用方法
    public <T> T queryOneLine(String sql, Class<T> clazz, Boolean underScoreToCamel) throws SQLException {

        if (underScoreToCamel) {
            T queryOne = runner.query(sql, new BeanHandler<T>(clazz, new BasicRowProcessor(new GenerousBeanProcessor())));

            return queryOne;
        } else {
            Map<String, Object> query = runner.query(sql, new MapHandler());

            JSONObject entries = JSONUtil.parseObj(query);

            return null;
        }

    }

    public void update(String sql) throws SQLException {
        runner.update(sql);
    }
}



