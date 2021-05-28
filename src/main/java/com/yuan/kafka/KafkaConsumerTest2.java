package com.yuan.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaConsumerTest2 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1.创建用于连接kafka的Properties
        Properties props=new Properties();
        props.put("bootstrap.servers","192.168.81.128:9092");
        //消费组，可以使用消费组将若干个消费者组织到一起，共同消费kafka中饭topic的数据
        props.put("group.id","yuan");
        //自动提交offset
        props.put("enable.auto.commit","true");

        props.put(".auto.commit.interval.ms","1000");

        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //创建一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //从哪个主题中拉去数据
        consumer.subscribe(Arrays.asList("third"));
        //使用一个while循环，不断从kafka的topic获取消息
        while(true){
            //一批一批的拉取
            ConsumerRecords<String, String> consumerRecords=consumer.poll(Duration.ofSeconds(5));
            //将记录
            for(ConsumerRecord<String, String> consumerRecord:consumerRecords){
                String topic=consumerRecord.topic();
                //分区位置
                long offset=consumerRecord.offset();
                String key=consumerRecord.key();
                String value=consumerRecord.value();
                int partition=consumerRecord.partition();
                System.out.println("topic:"+topic+" 分区id:"+partition+" offset:"+offset+" key:"+key+" value:"+value);
            }
        }
    }
}
