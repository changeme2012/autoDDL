package com.lc.utils;

import cn.hutool.json.JSONObject;
import com.lc.common.MyConfiguration;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * ClassName:MykafkaUtil
 * Package:com.lc.utils
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-04-25-15:30
 */
public class MykafkaUtil {



    public static KafkaSource<String> getKafkaSource(String topic, String groupId){

        return KafkaSource.<String>builder()
                .setBootstrapServers(MyConfiguration.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                //从上次提交的offset消费
//                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new DeserializationSchema<String>() {

                    @Override
                    public String deserialize(byte[] message) throws IOException {

                        if (message != null) {
                            return new String(message,StandardCharsets.UTF_8);
                        }else {
                            return null;
                        }
                    }
                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
//                        return  TypeInformation.of( new TypeHint<String>() {});
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                }))

/*                .setValueOnlyDeserializer(new DeserializationSchema<JSONObject>() {
                    @Override
                    public JSONObject deserialize(byte[] message) throws IOException {
                        if (message == null) {
                            return null;
                        }

                        return  JSONObject.parseObject(new String(message, StandardCharsets.UTF_8)) ;
                    }

                    @Override
                    public boolean isEndOfStream(JSONObject nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<JSONObject> getProducedType() {
                        return  TypeInformation.of(JSONObject.class);
                    }
                })*/
                .build();

    }


    //生产者
    public static  FlinkKafkaProducer<JSONObject> getFlinkKafkaProducer(String topic){

        return new FlinkKafkaProducer<JSONObject>(MyConfiguration.KAFKA_BOOTSTRAP_SERVERS,
                topic,
                element -> element.toJSONString(0).getBytes());
    }

    //建source表ddl kafka connect
    public static String getKafkaDDL(String topic,String groupId){
        return " WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" +topic+ "',\n" +
                "  'properties.bootstrap.servers' = '"+MyConfiguration.KAFKA_BOOTSTRAP_SERVERS+"',\n" +
                "  'properties.group.id' = '" +groupId+ "',\n" +
                "  'scan.startup.mode' = 'group-offsets',\n" +
                "  'format' = 'json'\n" +
                ")";
    }


    //建sink表ddl  kafka connect
    public static String getKafkaSinkDDL(String topic){
        return " WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" +topic+ "',\n" +
                "  'properties.bootstrap.servers' = '"+MyConfiguration.KAFKA_BOOTSTRAP_SERVERS+"',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    //原始json建表模板
    public static String getTopicDb(String groupId) {
        return "CREATE TABLE topic_db ( " +
                "  `database` STRING, " +
                "  `table` STRING, " +
                "  `type` STRING, " +
                "  `data` MAP<STRING,STRING>, " +
                "  `old` MAP<STRING,STRING>, " +
                "  `pt` AS PROCTIME() " +
                ") " + getKafkaDDL("topic_db", groupId);
    }

}
