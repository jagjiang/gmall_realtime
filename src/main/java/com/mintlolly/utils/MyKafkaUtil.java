package com.mintlolly.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created on 2022/3/14
 *
 * @author jiangbo
 * Description:将flink获取的数据推给kafka
 */
public class MyKafkaUtil {
    final static Logger log = LoggerFactory.getLogger(MyKafkaUtil.class);
    //kafka server
    private final static String KAFKA_BROKER = "101.42.251.112:9092";
    private final static String DEFAULT_TOPIC = "dwd_default_topic";
    //配置获取
    static Properties props = new Properties();

    static {
        props.setProperty("bootstrap.servers", KAFKA_BROKER);
    }

    //flink 将数据推给kafka
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        log.info("构建flink sink");
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), props);
    }

    //flink 消费kafka数据
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupid) {
        log.info("构建flink source");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupid);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
    }

    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,5*60*1000 + "");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC,kafkaSerializationSchema,props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
