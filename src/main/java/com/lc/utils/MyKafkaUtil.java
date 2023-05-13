package com.lc.utils;

import cn.hutool.json.JSONObject;
import com.lc.common.MyConfiguration;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * ClassName:MykafkaUtil
 * Package:com.lc.utils
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-04-25-15:30
 */
public class MyKafkaUtil {



    public static KafkaSource<String> getKafkaSource(String topic, String groupId){

        return KafkaSource.<String>builder()
                .setBootstrapServers(MyConfiguration.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
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


    //指定主题生产者
    public static  FlinkKafkaProducer<JSONObject> getFlinkKafkaProducer(String topic){

        return new FlinkKafkaProducer<JSONObject>(MyConfiguration.KAFKA_BOOTSTRAP_SERVERS,
                topic,
                new SerializationSchema<JSONObject>() {
                    @Override
                    public byte[] serialize(JSONObject element) {
                        return element.toJSONString(0).getBytes();
                    }
                });
    }

    //不指定主题生产者
    public static  FlinkKafkaProducer<JSONObject> getFlinkKafkaProducer(){

        KafkaSerializationSchema<JSONObject> serializationSchema = new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                //提取sink_table的值作为topic
                String sinkTable = element.getStr("sink_table");

                byte[] bytes = element.toString().getBytes(StandardCharsets.UTF_8);

                return new ProducerRecord<>(sinkTable, bytes);
            }
        };

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",MyConfiguration.KAFKA_BOOTSTRAP_SERVERS);

        return new FlinkKafkaProducer<JSONObject>(
                "defalut",
                serializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                );
    }
    //lookup表
    public static String getBaseDicDDL(){
        return "create table `base_dic`(\n" +
                "`dic_code` string,\n" +
                "`dic_name` string,\n" +
                "`parent_code` string,\n" +
                "`create_time` timestamp,\n" +
                "`operate_time` timestamp,\n" +
                "primary key(`dic_code`) not enforced\n" +
                ") WITH (\n" +
                "'connector' = 'jdbc',\n" +
                "'url' = 'jdbc:mysql://hadoop102:3306/gmall',\n" +
                "'table-name' = 'base_dic',\n" +
                "'lookup.cache.max-rows' = '10',\n" +
                "'lookup.cache.ttl' = '1 hour',\n" +
                "'username' = 'root',\n" +
                "'password' = '000000',\n" +
                "'driver' = 'com.mysql.cj.jdbc.Driver'\n" +
                ")";
    }

    //建source表ddl kafka connect
    public static String getKafkaSourceDDL(String topic,String groupId){
        return " WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" +topic+ "',\n" +
                "  'properties.bootstrap.servers' = '"+MyConfiguration.KAFKA_BOOTSTRAP_SERVERS+"',\n" +
                "  'properties.group.id' = '" +groupId+ "',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
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
    //建upsert sink表ddl  kafka connect
    public static String getKafkaUpsertSinkConnOption(String topic) {
        return "WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = '" + MyConfiguration.KAFKA_BOOTSTRAP_SERVERS + "', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
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
                "  `ts` BIGINT, " +
                "  `pt` AS PROCTIME() " +
                ") " + getKafkaSourceDDL("topic_db", groupId);
    }

}
