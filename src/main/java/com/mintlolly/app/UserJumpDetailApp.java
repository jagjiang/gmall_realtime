package com.mintlolly.app;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mintlolly.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created on 2022/4/8
 *
 * @author jiangbo
 * Description:
 */
public class UserJumpDetailApp {
    Logger log = LoggerFactory.getLogger(UniqueVisitApp.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);

        WatermarkStrategy<JsonNode> watermarkStrategy = WatermarkStrategy
                .<JsonNode>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<JsonNode>() {
                    @Override
                    public long extractTimestamp(JsonNode element, long recordTimestamp) {
                        return element.get("ts").asLong();
                    }
                });
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource.setStartFromEarliest());
        SingleOutputStreamOperator<JsonNode> jsonObjDS = kafkaDS
                .process(new ProcessFunction<String, JsonNode>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JsonNode> out) throws Exception {
                        JsonNode jsonNode = null;
                        try {
                            jsonNode = new ObjectMapper().readTree(value);
                            out.collect(jsonNode);
                        } catch (JsonProcessingException e) {
                            ctx.output(new OutputTag<>("dirty"), value);
                            e.printStackTrace();
                        }
                    }
                }).assignTimestampsAndWatermarks(watermarkStrategy);
        //4.按照 mid 分组
        KeyedStream<JsonNode, Object> keyedStream = jsonObjDS.keyBy(json -> {
            return json.get("common").get("mid").asText();
        });
        //通过Flink的CEP完成跳出判断
        Pattern<JsonNode, JsonNode> pattern = Pattern.<JsonNode>begin("begin")
                .where(new SimpleCondition<JsonNode>() {
                    @Override
                    public boolean filter(JsonNode value) throws Exception {
                        JsonNode jsonNode = value.get("page").get("last_page_id");
                        return jsonNode == null || jsonNode.textValue().length() <= 0;
                    }
                }).times(2) //默认使用宽松近邻
                .consecutive()  //指定使用严格近邻
                .within(Time.seconds(10));
        //将模式序列作用在流上
        PatternStream<JsonNode> patternStream = CEP.pattern(keyedStream, pattern);

        //提取事件和超时事件
        OutputTag<JsonNode> timeOutTag = new OutputTag<JsonNode>("TimeOut") {};
        SingleOutputStreamOperator<JsonNode> selectDS = patternStream.select(timeOutTag,new PatternTimeoutFunction<JsonNode, JsonNode>() {
            @Override
            public JsonNode timeout(Map<String, List<JsonNode>> map, long timeoutTimestamp) throws Exception {
                List<JsonNode> begin = map.get("begin");
                return begin.get(0);
            }
        }, new PatternSelectFunction<JsonNode, JsonNode>() {
            @Override
            public JsonNode select(Map<String, List<JsonNode>> map) throws Exception {
                return map.get("begin").get(0);
            }
        });
        DataStream<JsonNode> userJumpDetailDS = selectDS.getSideOutput(timeOutTag);
        DataStream<JsonNode> result = selectDS.union(userJumpDetailDS);
        result.print(">>>>>>>>>>>>>");
        result.map(JsonNode::toString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));
        env.execute();
    }
}
