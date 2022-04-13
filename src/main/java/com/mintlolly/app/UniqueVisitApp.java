package com.mintlolly.app;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mintlolly.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;

/**
 * Created on 2022/4/1
 *
 * @author jiangbo
 * Description:
 */
public class UniqueVisitApp {
    final Logger log = LoggerFactory.getLogger(UniqueVisitApp.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStateBackend(new FsStateBackend("hdfs://master:8020/gmall/dwd_log/ck"));
        //1.2 开启 CK
//        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.读取 Kafka dwd_page_log 主题数据创建流
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource.setStartFromEarliest());
        SingleOutputStreamOperator<JsonNode> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JsonNode>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JsonNode> out) {
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    JsonNode jsonNode = objectMapper.readTree(value);
                    out.collect(jsonNode);
                } catch (JsonProcessingException e) {
                    ctx.output(new OutputTag<>("dirty"), value);
                }
            }
        });
        //按照mid进行分组
        KeyedStream<JsonNode, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.get("common").get("mid").asText());
//        keyedStream.print("keyed Stream");
        //过滤掉不是今天第一次访问的数据
        SingleOutputStreamOperator<JsonNode> filter = keyedStream.filter(new RichFilterFunction<JsonNode>() {
            //声明状态
            private ValueState<String> firstVisitState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                ValueStateDescriptor<String> stringValueDescriptor = new ValueStateDescriptor<>("visit_state", String.class);
                //创建状态TTL配置项
                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                stringValueDescriptor.enableTimeToLive(stateTtlConfig);
                firstVisitState = getRuntimeContext().getState(stringValueDescriptor);
            }

            @Override
            public boolean filter(JsonNode value) throws Exception {
                //取出上一个访问的页面
                JsonNode last_page = value.get("page").get("last_page_id");
                //如果存在上一个页面则证明不是第一次访问，可以直接过滤
                if (last_page == null) {
                    //取出状态数据
                    String firstVisitDate = firstVisitState.value();
                    //取出数据时间
                    Long ts = value.get("ts").asLong();
                    String curDate = simpleDateFormat.format(ts);
                    if (firstVisitDate == null || !firstVisitDate.equals(curDate)) {
                        firstVisitState.update(curDate);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        });

        filter.map(JsonNode::toString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));
        env.execute();
    }
}
