package com.mintlolly.app;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mintlolly.app.func.DimSink;
import com.mintlolly.app.func.TableProcessFunction;
import com.mintlolly.bean.TableProcess;
import com.mintlolly.utils.MyKafkaUtil;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;


import javax.annotation.Nullable;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

/**
 * Created on 2022/3/21
 *
 * @author jiangbo
 * Description:
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1.1设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://master:8020/gmall/dwd_log/ck"));
        //1.2开启CK
//        env.enableCheckpointing(2000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.读取kafka数据
        String topic = "ods_base_db";
        String groupId = "ods_db_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> stringDataStreamSource = env.addSource(kafkaSource.setStartFromTimestamp(1647569000000L));
        //3.将每行数据转换成json
        SingleOutputStreamOperator<JsonNode> toJson = stringDataStreamSource.map(f -> new ObjectMapper().readTree(f));
        //4.过滤
        SingleOutputStreamOperator<JsonNode> afterFilter = toJson.filter(f -> f.get("data") != null);
        //5.创建mysql cdc source
        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("master")
                .username("root")
                .password("123456")
                .databaseList("gmall_realtime")
                .tableList("gmall_realtime.table_process")
                .deserializer(new DebeziumDeserializationSchema<String>() {
                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
                        String topic = sourceRecord.topic();
                        String[] dbTable = topic.split("\\.");
                        String db = dbTable[1];
                        String tableName = dbTable[2];
                        Struct value = (Struct) sourceRecord.value();
                        Struct after = value.getStruct("after");
                        //创建json对象，用于存放数据信息
                        StringWriter stringWriter = new StringWriter();
                        JsonGenerator result = new com.fasterxml.jackson.databind.ObjectMapper().getFactory().createGenerator(stringWriter);
                        HashMap<String, Object> data = new HashMap<>();
                        if (after != null) {
                            Schema schema = after.schema();
                            for (Field field : schema.fields()) {
                                data.put(field.name(), after.get(field.name()));
                            }
                        }
                        //获取操作类型

                        //创建json对象，用于存放最终返回值数据信息
                        result.writeStartObject();
                        result.writeObjectField("data", data);
                        result.writeStringField("type", operation.toString().toLowerCase());
                        result.writeStringField("database", db);
                        result.writeStringField("table", tableName);
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
                }).build();
        //6.读取mysql数据
        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);
        tableProcessDS.print();
        //7.定义一个MapStateDescriptor来描述我们要广播的数据的格式
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table-process-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessDS.broadcast(mapStateDescriptor);
        //8.将主流和广播流进行连接
        BroadcastConnectedStream<JsonNode, String> connectedStream = afterFilter.connect(broadcastStream);

        OutputTag<JsonNode> hbaseTag = new
                OutputTag<JsonNode>(TableProcess.SINK_TYPE_HBASE) {
                };
        SingleOutputStreamOperator<JsonNode> kafkaJsonDS = connectedStream.process(new
                TableProcessFunction(hbaseTag, mapStateDescriptor));
        DataStream<JsonNode> hbaseJsonDS = kafkaJsonDS.getSideOutput(hbaseTag);
        FlinkKafkaProducer<JsonNode> kafkaSinkBySchema = MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JsonNode>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JsonNode element, @Nullable Long timestamp) {
                String data = element.get("data").toString();
                return new ProducerRecord<byte[], byte[]>(element.get("sink_table").textValue(), data.getBytes(StandardCharsets.UTF_8));
            }
        });
        kafkaJsonDS.addSink(kafkaSinkBySchema);
        hbaseJsonDS.addSink(new DimSink());

        env.execute("BaseDBApp job");
    }
}
