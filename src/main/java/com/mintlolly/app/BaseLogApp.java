package com.mintlolly.app;

import com.mintlolly.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Iterator;

/**
 * Created on 2022/3/15
 *
 * @author jiangbo
 * Description:
 */
public class BaseLogApp {
    Logger log = LoggerFactory.getLogger(BaseLogApp.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://master:8020/baselogck"));
        System.setProperty("HADOOP_USER_NAME","hadoop");
        String topic = "ods_base_log";
        String groupid = "test";
        env.setParallelism(1);
        DataStreamSource<String> dss = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupid).setStartFromTimestamp(1647569000111L));
        KeyedStream<String, String> keyedStream = dss.keyBy(f -> {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readValue(f, JsonNode.class);
            return jsonNode.get("common").get("mid").getTextValue();
        });
//        StreamingFileSink<JsonNode> sink = StreamingFileSink.forRowFormat(new Path("hdfs://master:8020/sinkout2"), new SimpleStringEncoder<JsonNode>("UTF-8")).build();
//        dss.map(f -> new ObjectMapper().readTree(f)).addSink(sink);
        SingleOutputStreamOperator<JsonNode> jsonWithNewFlagDS = keyedStream.map(new RichMapFunction<String, JsonNode>() {
            //声明状态用于表示当前mid是否已经访问过
            private ValueState<String> firstVisitDateState;
            private SimpleDateFormat simpleDateFormat;
            @Override
            public void open(Configuration parameters) throws Exception {
                firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("new_mid",String.class));
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }
            @Override
            public JsonNode map(String f) throws Exception {
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode value = objectMapper.readValue(f, JsonNode.class);
                //取出新用户标记
                String isNew = value.get("common").get("is_new").getTextValue();
                //如果当前前端传输数据表示为新用户,则进行校验
                if ("1".equals(isNew)) {
                    //取出状态数据并取出当前访问时间
                    String firstDate = firstVisitDateState.value();
                    long ts = value.get("ts").getLongValue();
                    //判断状态数据是否为 Null
                    if (null != firstDate) {
                        //修复
                        ((ObjectNode) value.get("common")).put("is_new", "0");
                    } else {
                        //更新状态
                        firstVisitDateState.update(simpleDateFormat.format(ts));
                    }
                }
                return value;
            }
        });
        SingleOutputStreamOperator<String> pageDS = jsonWithNewFlagDS.process(new ProcessFunction<JsonNode, String>() {
            @Override
            public void processElement(JsonNode value, Context ctx, Collector<String> out) throws Exception {
                //提取start字段
                JsonNode startStr = value.get("start");
                //判断是否为启动数据
                if (startStr != null) {
                    ctx.output(new OutputTag<String>("start") {
                    }, value.toString());
                } else {
                    out.collect(value.toString());
                    JsonNode displays = value.get("displays");
                    if (displays != null) {
                        Iterator<JsonNode> displaysElements = displays.getElements();
                        while (displaysElements.hasNext()) {
                            JsonNode displaysJson = displaysElements.next();
                            ((ObjectNode) displaysJson).put("page_id", value.get("page").get("page_id"));
                            ctx.output(new OutputTag<String>("displays") {
                            }, displaysJson.toString());
                        }
                    }
                }
            }
        });

        DataStream<String> displayDS = pageDS.getSideOutput(new OutputTag<String>("displays") {});
        DataStream<String> startDS = pageDS.getSideOutput(new OutputTag<String>("start") {});

        pageDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));
        startDS.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));

        env.execute("Base log App");
    }
}
