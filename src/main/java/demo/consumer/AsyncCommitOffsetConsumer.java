package demo.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * 异步提交offset
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/4/16 23:56
 **/
public class AsyncCommitOffsetConsumer {

    public static void main(String[] args) {

        Properties props = getProperties();

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("first","second"));

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecord = consumer.poll(2000);
                for (TopicPartition topicPartition : consumerRecord.partitions()) {
                    List<ConsumerRecord<String, String>> recordList = consumerRecord.records(topicPartition);
                    for (ConsumerRecord<String, String> record : recordList) {
                        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                    }
                    long lastOffset = recordList.get(recordList.size() - 1).offset();

                    consumer.commitAsync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(lastOffset + 1)),(offsets, exception)->{
                        if(Objects.isNull(exception)){
                            System.out.println(offsets);
                        }else{
                            exception.getStackTrace();
                        }
                    });
                }
            }
        }finally {
            try{
                consumer.commitSync();
            }finally {
                consumer.close();
            }
        }
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        //kafka broker集群
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092,kafka2:9092,kafka3:9092");
        //消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-transaction-1");
        //开启自动提交offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //默认读取的offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //隔离级别
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");
        //key 反序列化类
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        //value 反序列化类
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
