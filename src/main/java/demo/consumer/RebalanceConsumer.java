package demo.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * 触发消费者Rebalance分区分配
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/4/16 23:56
 **/
public class RebalanceConsumer {

    public static void main(String[] args) {

        Properties props = getProperties();

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);

        Map<TopicPartition, OffsetAndMetadata> currentOffsetMap = new HashMap<>(16);

        consumer.subscribe(Arrays.asList("first", "second"), new ConsumerRebalanceListener() {
            // 发生分区分配之前
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                //提交当前offset
                consumer.commitSync(currentOffsetMap);
                currentOffsetMap.clear();
            }
            // 分区分配已完成
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            }
        });

        while (true){
            ConsumerRecords<String, String> consumerRecord = consumer.poll(100);
            for (TopicPartition topicPartition : consumerRecord.partitions()) {
                List<ConsumerRecord<String, String>> recordList = consumerRecord.records(topicPartition);
                for (ConsumerRecord<String, String> record : recordList) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
                long lastOffset = recordList.get(recordList.size() - 1).offset();
                currentOffsetMap.put(topicPartition,new OffsetAndMetadata(lastOffset + 1));
            }
            consumer.commitAsync(currentOffsetMap,null);
        }
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        //kafka broker集群
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092,kafka2:9092,kafka3:9092");
        //消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-transaction-2");
        //开启自动提交offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //默认读取的offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
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
