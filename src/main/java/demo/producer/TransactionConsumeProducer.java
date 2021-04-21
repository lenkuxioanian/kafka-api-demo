package demo.producer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/4/16 22:55
 **/
public class TransactionConsumeProducer {

    /**
     * 来源topic
     */
    private static final String SOURCE_TOPIC = "first";
    /**
     * 目标topic
     */
    private static final String TARGET_TOPIC = "third";

    public static void main(String[] args) {

        Properties producerProps = getProducerProperties();
        Properties consumerProps = getConsumerProperties();
        KafkaProducer<String,String> producer = new KafkaProducer<>(producerProps);
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(TARGET_TOPIC));
        //初始化事务
        producer.initTransactions();
        while (true){
            ConsumerRecords<String, String> consumerRecord = consumer.poll(2000);
            if(!consumerRecord.isEmpty()){
                Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>(16);
                //开启事务
                producer.beginTransaction();
                try{
                    for(TopicPartition topicPartition : consumerRecord.partitions()){
                        List<ConsumerRecord<String, String>> records = consumerRecord.records(topicPartition);
                        for(ConsumerRecord<String, String> record : records){
                            System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                            producer.send(new ProducerRecord<>(SOURCE_TOPIC,"kafka" + record.value())).get();
                        }
                        long lastOffset = records.get(records.size() - 1).offset();
                        offsetAndMetadataMap.put(topicPartition,new OffsetAndMetadata(lastOffset + 1));
                    }
                    //提交offset
                    producer.sendOffsetsToTransaction(offsetAndMetadataMap,"test-transaction");
                    //提交事务
                    producer.commitTransaction();
                }catch (Exception e){
                    //终止事务
                    producer.abortTransaction();
                }
            }
        }

    }

    private static Properties getProducerProperties() {
        Properties props = new Properties();
        // kafka broker集群
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"kafka1:9092,kafka2:9092,kafka3:9092");
        //批次大小
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //等待时间
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //RecordAccumulator 缓冲区大小
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        //开启幂等性
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        //transactional.id
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"test-t-id");
        //key 序列化类
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        //value 序列化类
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    private static Properties getConsumerProperties() {
        Properties props = new Properties();
        //kafka broker集群
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092,kafka2:9092,kafka3:9092");
        //消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-transaction");
        //开启自动提交offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //默认读取的offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        //key 反序列化类
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        //value 反序列化类
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
