package com.mintlolly.review.flink;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * Created on 2022/4/18
 *
 * @author jiangbo
 * Description:
 */
public class WindowAndWaterMarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //socket source
        DataStreamSource<String> ds = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<String> ssoso = ds.assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                .withTimestampAssigner(new MyTimeAssigner()));
        KeyedStream<String, String> ks = ssoso.keyBy(f -> f.split(",")[0]);
        ks.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new AddAgeAggregate()).print("watermark");

//        ks.window(TumblingProcessingTimeWindows.of(Time.seconds(10))).aggregate(new AddAgeAggregate()).print("Tumbling");
//        ks.window(SlidingProcessingTimeWindows.of(Time.seconds(20), Time.seconds(5))).aggregate(new AddAgeAggregate()).print("Sliding");

        env.execute();

        //设置watermark
    }
     static class MyTimeAssigner implements SerializableTimestampAssigner<String> {

        @Override
        public long extractTimestamp(String element, long recordTimestamp) {
            return Long.parseLong(element.split(",")[2]);
        }
    }

    static class AddAgeAggregate implements AggregateFunction<String, String, String> {

        @Override
        public String createAccumulator() {
            return "0,0,0";
        }

        @Override
        public String add(String value, String acc) {
            String[] split = value.split(",");

            int sum = Integer.parseInt(acc.split(",")[1]) + Integer.parseInt(split[1]);
            return split[0] + "," + sum + ","+split[2];
        }

        @Override
        public String getResult(String acc) {
            return acc;
        }

        @Override
        public String merge(String a, String b) {
            int sum = Integer.parseInt(a.split(",")[1]) + Integer.parseInt(b.split(",")[1]);
            return a.split(",")[0] + "," + sum+ ","+a.split(",")[2];
        }
    }
}
