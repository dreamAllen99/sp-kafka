package com.dram.spring.io.kafka;

import ch.qos.logback.core.util.TimeUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;

import java.util.HashMap;
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
                logger.error("\n\n\n\n==receive mesage==>{}\n\n\n" , data);
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
        latch.await(2, TimeUnit.SECONDS);

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
        Map<String,Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"c6:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group01");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,100);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return properties;
    }

    private Map<String,Object> getProducerProperties(){
        Map<String,Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"c6:9092");
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,IntegerDeserializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        return properties;
    }

    private KafkaTemplate<Integer,String>  createTemplate(){


        ProducerFactory producerFactory = new DefaultKafkaProducerFactory(getProducerProperties());

        KafkaTemplate<Integer,String> template = new KafkaTemplate<Integer, String>(producerFactory);

        return template;
    }


    public static void main(String[] args) throws Exception {
        PlainKafkaUtil p = new PlainKafkaUtil();
        p.simpleDemo4ProduceAndConume();

    }


}
