package com.lc.utils;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.json.JSONObject;
import com.lc.common.MyConfiguration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * ClassName:PhoenixUtil
 * Package:com.lc.utils
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-04-26-20:24
 */
public class PhoenixUtil {

    public static void writeValues(Connection connection, String sinkTable, JSONObject data) throws SQLException {



        //1.rowkey为sinkTable column为data字段
        //拼接SQL语句:upsert into db.tn(id,name,sex) values('1001','zhangsan','male')
        /**
         * {"id": 50156,"consignee": "陈馥筠","consignee_tel": "13433583474","total_amount": 99.00,"order_status": "1002","user_id": 1,"payment_way": "3501","delivery_address": null,"order_comment": null,"out_trade_no": "194162343458392","trade_body": "索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Z03女王红 性感冷艳 璀璨金钻哑光唇膏 等1件商品","create_time": "2022-06-08 15:18:26","operate_time": "2022-06-08 15:18:26","expire_time": "2022-06-08 15:28:26","process_status": null,"tracking_no": null,"parent_order_id": null,"img_url": null,"province_id": 4,"activity_reduce_amount": 0.00,"coupon_reduce_amount": 30.00,"original_total_amount": 129.00,"feight_fee": null,"feight_fee_reduce": null,"refundable_time": "2022-06-15 15:18:26"}
         */
        //拼接表名
        Set<String> columnName = data.keySet();

        Collection<Object> values = data.values();

        String sql = "upsert into "+ MyConfiguration.HBASE_SCHEMA + "." +sinkTable + "(" +
                CollUtil.join(columnName,",") +") " +"values('" +
                CollUtil.join(values,"','") + "')";

        //打印sql
        System.out.println("phoenix写入语句 "+sql);

        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        preparedStatement.execute();


        preparedStatement.close();

    }
}
