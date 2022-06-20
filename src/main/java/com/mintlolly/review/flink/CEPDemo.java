package com.mintlolly.review.flink;

import com.mintlolly.review.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created on 2022/5/1
 *
 * @author jiangbo
 * Description:Complex Event Processing 复杂事件处理
 */
public class CEPDemo {
    Logger LOG = LoggerFactory.getLogger(CEPDemo.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KeyedStream<LoginEvent, String> keyedStream = env.fromElements(new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                new LoginEvent("user_2", "192.168.1.29", "success", 6000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }))
                .keyBy(LoginEvent::getUserId);

        //连续3个登录失败的事件
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.getEventType().equals("fail");
                    }
                })
                .next("second").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.getEventType().equals("fail");
                    }
                })
                .next("third").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.getEventType().equals("fail");
                    }
                });

        //将CEP应用到流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, pattern);
        //将匹配到的事件提取出来，包装成报警信息
        patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) {
                LoginEvent first = map.get("first").get(0);
                LoginEvent second = map.get("second").get(0);
                LoginEvent third = map.get("third").get(0);
                return first.getUserId() + " 连续三次登录失败！登录时间：" +
                        first.getTimestamp() + ", " + second.getTimestamp() + ", " + third.getTimestamp();
            }
        }).print("warning");

        //匹配事件出现3次，宽松近临，中间可以有其他事件  ***加入consecutive之后，一旦中间出现了不匹配的条件，当前循环检测就会终止
        Pattern<LoginEvent, LoginEvent> times = Pattern.<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.getEventType().equals("fail");
                    }
                })
                .times(3)
                .consecutive();

        CEP.pattern(keyedStream, times)
                .select(new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> map) throws Exception {
                        LoginEvent first = map.get("first").get(0);
                        LoginEvent second = map.get("first").get(1);
                        LoginEvent third = map.get("first").get(2);
                        return first.getUserId() + " 三次登录失败！登录时间：" +
                                first.getTimestamp() + ", " + second.getTimestamp() + ", " + third.getTimestamp();
                    }
                }).print("warning2");

        Pattern.<LoginEvent>begin("first")
                .oneOrMore()
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                        return false;
                    }
                });
        env.execute();
    }
}
