import com.lc.app.ddl.KafkaTable;
import com.lc.common.Functions;

/**
 * ClassName:Test01
 * Package:PACKAGE_NAME
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-05-11-11:37
 */
public class Test01 {
    public static void main(String[] args) {



/*        KafkaTable build = KafkaTable.builder()
//                .topic("topic_db")
//                .groupId("gmall_221109")
                .database("gmall")
                .table("cart_info")
                .exclude("img_url")
                .columnExp("create_time",Functions.UNIX_TIMESTAMP)

     *//*           .scanMode(MyConfiguration.LATEST_OFFSET)
                .maxwelSourceColumn()
                .kafkaSourceWith()*//*

                .build();*/

//cart_info表
/*
        KafkaTable cartInfo = KafkaTable.builder()
                .database("gmall")
                .table("cart_info")
//                .exclude("img_url,is_checked")
                .include("id,name")
                .columnExp("create_time",Functions.UNIX_TIMESTAMP)
                .columnExp("operate_time",Functions.UNIX_TIMESTAMP)
                .filterTable("topic_db","cart_info","insert")
                .saveTableAlias("dwd_cart_add")
                .build();
*/

        //cart_info表
        KafkaTable userInfo = KafkaTable.builder()
                .database("gmall")
                .table("user_info")
//                .exclude("img_url,is_checked")
                .include("id,name")
                .function("id",Functions.UNIX_TIMESTAMP)
                .function("name",Functions.UNIX_TIMESTAMP)
//                .filterTable("topic_db","cart_info","insert")
                .select("user_info")
//                .saveTableAlias("dwd_cart_add")
                .build();


        userInfo.print();
    /*    String qurey = build.qurey();
        System.out.println(qurey);*/
    }
}