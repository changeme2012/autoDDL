package com.lc.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * ClassName:SkipField
 * Package:com.lc.bean
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-05-04-14:03
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ReduceField {
}
