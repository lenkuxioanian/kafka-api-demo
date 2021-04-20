package demo.consumer;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 指定topic分区消费
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/4/16 23:56
 **/
public class PartitionConsumer {

    public static void main(String[] args) {

        Properties props = getProperties();
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        List<TopicPartition> partitions = new ArrayList<>(16);
        // 获取topic分区信息
        List<PartitionInfo> partitionInfos = consumer.partitionsFor("third");
        if(!CollectionUtils.isEmpty(partitionInfos)) {
            for (PartitionInfo partitionInfo : partitionInfos) {
                partitions.add(new TopicPartition(partitionInfo.topic(),partitionInfo.partition()));
            }
        }
        // 指定topic分区进行消费
        consumer.assign(partitions);

        while (true){
            ConsumerRecords<String, String> consumerRecord = consumer.poll(2000);
            for(ConsumerRecord<String, String> record : consumerRecord){
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        //kafka broker集群
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092,kafka2:9092,kafka3:9092");
        //消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-demo");
        //开启自动提交offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //自动提交offset间隔
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        //默认读取的offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //key 反序列化类
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        //value 反序列化类
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
