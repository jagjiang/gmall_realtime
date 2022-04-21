package com.mintlolly.app;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mintlolly.utils.MyKafkaUtil;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.HashMap;

/**
 * Created on 2022/3/14
 *
 * @author jiangbo
 * Description:
 */
public class Flink_CDCWithCustomerSchema {
    static final Logger log = LoggerFactory.getLogger(Flink_CDCWithCustomerSchema.class);

    public static void main(String[] args) throws Exception {
        log.info("创建执行环境");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //将读取binlog的位置信息以状态的形式保存到CK，如果想要断点续传需要从Checkpoint或者Savepoint启动程序
//        //开启checkpoint，每3秒执行一次
        env.enableCheckpointing(3000);
//        //指定checkpoint的一致性语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //设置任务关闭的时候保留最后一次的checkpoint数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //指定从ck自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
//        //设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://master:8020/checkpoint"));
//        //设置访问hdfs的用户名
//        System.setProperty("HADOOP_USER_NAME","hadoop");
        log.info("创建flink mysql cdc的source");
        SourceFunction<String> mySqlSource = MySQLSource.<String>builder()
                .hostname("101.42.251.112")
                .serverTimeZone("Asia/Shanghai")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall")
//                .tableList("activity_info")
                .deserializer(new DebeziumDeserializationSchema<String>() {
                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
                        //获取操作类型
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
                        //获取主题信息，包含数据库和表名
                        String topic = sourceRecord.topic();
                        String[] arr = topic.split("\\.");
                        String db = arr[1];
                        String tableName = arr[2];
                        //获取值信息，并转换为Struct类型
                        Struct value = (Struct) sourceRecord.value();
                        //获取变化后的数据
                        Struct after = value.getStruct("after");
                        //创建json对象，用于存放数据信息
                        StringWriter stringWriter = new StringWriter();
                        JsonGenerator result = new ObjectMapper().getFactory().createGenerator(stringWriter);
                        HashMap<String,Object> data= new HashMap<>();
                        if(after != null){
                            Schema schema = after.schema();
                            for (Field field : schema.fields()) {
                                data.put(field.name(),after.get(field.name()));
                            }
                        }
                        //创建json对象，用于存放最终返回值数据信息
                        result.writeStartObject();
                        result.writeObjectField("data",data);
                        result.writeStringField("operation",operation.toString().toLowerCase());
                        result.writeStringField("dbname",db);
                        result.writeStringField("table",tableName);
                        result.writeEndObject();
                        result.flush();
                        result.close();
                        //发送数据到下游
                        collector.collect(stringWriter.toString());
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();



        log.info("使用CDC source从mysql读取数据");
        DataStreamSource<String> stringDataStreamSource = env.addSource(mySqlSource);
        stringDataStreamSource.addSink(MyKafkaUtil.getKafkaSink("ods_base_db"));
        log.info("执行任务");
        env.execute("Print MySQL Snapshot + Binlog");
    }
}
