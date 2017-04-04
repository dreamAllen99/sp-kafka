package com.dram.spring.io.kafka;

import ch.qos.logback.core.util.TimeUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by allen on 17/4/4.
 */
public class PlainKafkaUtil {

    private final static Logger logger = LoggerFactory.getLogger(PlainKafkaUtil.class);

    public void simpleDemo4ProduceAndConume() throws Exception {

        final CountDownLatch latch = new CountDownLatch(2);

        ContainerProperties containerProperties = new ContainerProperties("topic1","topic2");
        containerProperties.setMessageListener(new MessageListener<Integer,String>() {
            public void onMessage(ConsumerRecord<Integer, String> data) {
                logger.warn("==receive mesage==>" + data);
                latch.countDown();
            }
        });

        KafkaMessageListenerContainer<Integer,String> container = createContainer(containerProperties);

        container.setBeanName("testAuto");

        container.start();

        Thread.sleep(1000); // wait a bit for the container to start


        KafkaTemplate<Integer,String> producerTemplate = createTemplate();
        producerTemplate.setDefaultTopic("topic1");
        producerTemplate.sendDefault(0, "aa");
        producerTemplate.sendDefault(1,"bb");
        producerTemplate.sendDefault(0,"cc");
        producerTemplate.sendDefault(1,"dd");

        // send msg
        producerTemplate.flush();

        //
        latch.await(60, TimeUnit.SECONDS);

        container.stop();

        logger.warn("-----End------");

    }


    private KafkaMessageListenerContainer<Integer,String> createContainer(ContainerProperties containerProperties){
        Map<String,Object> consumerProperties = getConsumerProperties();
        DefaultKafkaConsumerFactory<Integer,String> consumerFactory = new DefaultKafkaConsumerFactory<Integer, String>(consumerProperties);
        KafkaMessageListenerContainer<Integer,String> container = new KafkaMessageListenerContainer<Integer, String>(consumerFactory,containerProperties);
        return container;
    }

    private Map<String,Object> getConsumerProperties(){

        return null;
    }

    private Map<String,Object> getProducerProperties(){

        return null;
    }

    private KafkaTemplate<Integer,String>  createTemplate(){

        DefaultKafkaProducerFactory producerFactory = new DefaultKafkaProducerFactory(getProducerProperties());

        KafkaTemplate<Integer,String> template = new KafkaTemplate<Integer, String>(producerFactory);

        return template;
    }


    public static void main(String[] args) {


    }


}
