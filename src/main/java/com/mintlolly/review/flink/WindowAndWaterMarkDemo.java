package com.mintlolly.review.flink;

import com.mintlolly.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * Created on 2022/4/18
 *
 * @author jiangbo
 * Description:
 */
public class WindowAndWaterMarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource("windows", "test");
        DataStreamSource<String> ds = env.addSource(kafkaSource.setStartFromEarliest());
        //设置watermark
        SingleOutputStreamOperator<String> ssoso = ds.assignTimestampsAndWatermarks(WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new MyTimeAssigner()));
        //keyby 打印窗口信息
        KeyedStream<String, String> ks = ssoso.keyBy(f -> f.split(",")[0]);

        ks.map(f-> {
            String[] split = f.split(",");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
            String date = sdf.format(new Date(Long.parseLong(split[2])));
            return split[0] + "," +split[1] + "," +date;
        }).print("keyby");

        OutputTag<String> outputTag = new OutputTag<String>("late"){};

        SingleOutputStreamOperator<String> result = ks.window(TumblingEventTimeWindows.of(Time.seconds(20)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .aggregate(new AddAgeAggregate(), new MyWindowFunction());

        result.print("window");
        result.getSideOutput(outputTag).print("late");
        //滚动窗口
//        ks.window(TumblingProcessingTimeWindows.of(Time.seconds(10))).aggregate(new AddAgeAggregate()).print("Tumbling");
//      //滑动窗口
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
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            String date = sdf.format(new Date(Long.parseLong(split[2])));
            return split[0] + "," + sum+ ","+date;
        }

        @Override
        public String getResult(String acc) {
            return acc;
        }

        @Override
        public String merge(String a, String b) {
            int sum = Integer.parseInt(a.split(",")[1]) + Integer.parseInt(b.split(",")[1]);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
            String date = sdf.format(new Date(Long.parseLong(a.split(",")[2])));
            return a.split(",")[0] + "," + sum+ ","+date;
        }
    }

    private static class MyWindowFunction implements WindowFunction<String,String,String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow window, Iterable<String> input, Collector<String> out) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            String start = sdf.format(new Date(window.getStart()));
            String end = sdf.format(new Date(window.getEnd()));
            out.collect(start+"-"+end+","+input.iterator().next());
        }
    }
}
