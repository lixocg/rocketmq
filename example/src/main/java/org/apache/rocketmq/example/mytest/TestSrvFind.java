package org.apache.rocketmq.example.mytest;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MixAll;

public class TestSrvFind {
    public static void main(String[] args) throws MQClientException {
//        DefaultMQProducer producer = new DefaultMQProducer("Producer");
//        producer.setNamesrvAddr("127.0.0.1:9876");
//        producer.start();

        System.out.println(System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV)));
    }
}
