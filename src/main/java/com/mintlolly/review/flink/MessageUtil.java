package com.mintlolly.review.flink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Random;

/**
 * Created on 2022/4/24
 *
 * @author jiangbo
 * Description:
 */
public class MessageUtil {
    public static void main(String[] args) {
        String broker = "101.42.251.112:9092";
        String topic = "windows";
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,broker);
        props.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        //多线程发送数据
        setMessageThread(topic,producer);
        setMessageThread(topic,producer);
    }
    static String getMessage(){
        String[] names = {"lan", "black", "kee", "yang"};
        Random random = new Random();
        String name = names[random.nextInt(names.length)];
        int score = random.nextInt(10);
        long date = System.currentTimeMillis();
        return name + ","+score+","+date;
    }

    static void sendSocketMessage(String message) throws IOException {
        Socket socket = new Socket("101.42.251.112",9999);
        OutputStream out = socket.getOutputStream();
        out.write(message.getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    static void setMessageThread(String topic,KafkaProducer<String,String> producer){
        Random random = new Random();
        new Thread(() -> {
            for (int i = 0; i < 5; i++) {
                String message = getMessage();
                System.out.println(message);
                producer.send(new ProducerRecord<>(topic, message));
                producer.flush();
                try {
                    Thread.sleep(random.nextInt(10) * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
