package com.mintlolly.app;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mintlolly.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created on 2022/4/8
 *
 * @author jiangbo
 * Description:
 */
public class UserJumpDetailApp {
    Logger log = LoggerFactory.getLogger(UniqueVisitApp.class);
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);

        WatermarkStrategy<JsonNode> watermarkStrategy = WatermarkStrategy.<JsonNode>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JsonNode>() {
            @Override
            public long extractTimestamp(JsonNode element, long recordTimestamp) {
                return element.get("ts").asLong();
            }
        });

        env.addSource(kafkaSource).process(new ProcessFunction<String, JsonNode>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JsonNode> out) throws Exception{
                JsonNode jsonNode = null;
                try {
                    jsonNode = new ObjectMapper().readTree(value);
                    out.collect(jsonNode);
                } catch (JsonProcessingException e) {
                    ctx.output(new OutputTag<>("dirty"),value);
                    e.printStackTrace();
                }
            }
        }).assignTimestampsAndWatermarks(watermarkStrategy);
    }
}
