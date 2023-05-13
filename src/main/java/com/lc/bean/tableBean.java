package com.lc.bean;

import lombok.Data;

/**
 * ClassName:tableBean
 * Package:com.lc.bean
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-04-28-17:08
 */
@Data
public class tableBean {
    String tableName;
    String columnName;
    String dataType;
    String columnComment;
    String columnKey;
}
