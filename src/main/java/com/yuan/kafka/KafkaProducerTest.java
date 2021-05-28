package com.yuan.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1.创建用于连接kafka的Properties
        Properties props=new Properties();
        props.put("bootstrap.servers","192.168.81.128:9092");
        props.put("acks","1");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //创建一个生产者
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        //发送1-100消息
        for(int i=0;i<100;++i){
            //1.同步等待 get()方法实现
//            Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>("second", i + "", i + "消息"));
//            future.get();
            //2.异步回调
            ProducerRecord<String,String> producerRecord=new ProducerRecord("third",i+"消息");
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //1.判断发送消息是否成功
                    if(e==null){
                        //1.发送成功
                        String topic=recordMetadata.topic();
                        //分区id
                        int partition=recordMetadata.partition();
                        //偏移量
                        long offset= recordMetadata.offset();


                        System.out.println("topic:"+topic+" 分区id:"+partition+" 偏移量："+offset);
                    }else{
                        //阿松出现错误
                        System.out.println("生产消息出现错误");
                        //打印异常消息
                        System.out.println(e.getMessage());
                        //打印调用zhan
                        e.printStackTrace();

                    }
                }
            });

        }
        producer.close();

    }
}
