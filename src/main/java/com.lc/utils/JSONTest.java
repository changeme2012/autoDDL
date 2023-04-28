package com.lc.utils;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;

/**
 * ClassName:JSONTest
 * Package:com.lc.utils
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-04-27-15:28
 */
public class JSONTest {
    public static void main(String[] args) {

        String json = "{\"common\":{\"ar\":\"10\",\"ba\":\"iPhone\",\"ch\":\"Appstore\",\"is_new\":\"1\",\"md\":\"iPhone 14 Plus\",\"mid\":\"mid_485\",\"os\":\"iOS 13.3.1\",\"sid\":\"3f1f00b4-f7b0-4abf-990f-cc26e6b1ba84\",\"uid\":\"1\",\"vc\":\"v2.1.111\"},\"page\":{\"during_time\":13534,\"item\":\"1,19\",\"item_type\":\"sku_ids\",\"last_page_id\":\"cart\",\"page_id\":\"trade\"},\"ts\":1654673053000}";

        JSONObject jsonObject = JSONUtil.parseObj(json);

        if (jsonObject.containsKey("page")){
            jsonObject.remove("page");
        }

        System.out.println(jsonObject);


    }
}
