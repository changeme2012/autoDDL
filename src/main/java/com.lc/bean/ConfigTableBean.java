package com.lc.bean;

import lombok.Data;

/**
 * ClassName:ConfigTable
 * Package:com.lc.bean
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-04-25-16:47
 */
@Data
public class ConfigTableBean {
    //来源表
    String sourceTable;
    //输出表
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String SinkExtend;


}
