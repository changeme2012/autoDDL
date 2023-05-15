import com.lc.app.ddl.KafkaTable;

/**
 * ClassName:Test01
 * Package:PACKAGE_NAME
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-05-11-11:37
 */
public class Test01 {
    public static void main(String[] args) throws Exception {



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
                .table("cart_info")
//                .exclude("img_url,is_checked")
                .filterTable("topic_db","cart_info","insert")

                .saveTableAlias("dwd_cart_add")
                .build();


        KafkaTable cartAdd = KafkaTable.builder()
                .database("gmall")
                .table("cart_info")
//                .exclude("img_url,is_checked")
                .select("dwd_cart_add")

                .saveTableAlias("dwd_cart_add")
                .build();



//        userInfo.print();

        cartAdd.print();

    /*    String qurey = build.qurey();
        System.out.println(qurey);*/
    }
}
