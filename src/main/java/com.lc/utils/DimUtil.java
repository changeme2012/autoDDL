package com.lc.utils;

import cn.hutool.db.nosql.redis.RedisDS;
import redis.clients.jedis.Jedis;

/**
 * ClassName:DimUtil
 * Package:com.lc.utils
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-04-26-20:12
 */
public class DimUtil {


    public static void redisDel(String tableName, String key) {
        //通过hutool获取jedis连接
        Jedis jedis = RedisDS.create().getJedis();
        //删除数据
        jedis.del("DIM:" + tableName + ":" + key);
        //关闭连接
        jedis.close();

    }


}
