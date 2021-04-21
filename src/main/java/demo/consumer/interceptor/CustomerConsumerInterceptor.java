package demo.consumer.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 自定义消费者过滤器
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/4/16 23:56
 **/
public class CustomerConsumerInterceptor implements ConsumerInterceptor<String, String> {
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecordsMap = new HashMap<>(16);
        for (TopicPartition topicPartition : records.partitions()) {
            List<ConsumerRecord<String, String>> newRecords = new ArrayList<>(16);
            List<ConsumerRecord<String, String>> recordList = records.records(topicPartition);
            for (ConsumerRecord<String, String> record : recordList) {
                ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>(record.topic(),
                        record.partition(),
                        record.offset(),
                        record.timestamp(),
                        record.timestampType(),
                        record.checksum(),
                        record.serializedKeySize(),
                        record.serializedValueSize(),
                        record.key(),
                        "Hello " + record.value(),
                        record.headers());
                newRecords.add(consumerRecord);
            }
            newRecordsMap.put(topicPartition, newRecords);
        }
        return new ConsumerRecords<>(newRecordsMap);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((topicPartition, offsetAndMetadata) -> System.out.println(topicPartition +":"+ offsetAndMetadata.offset()));
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
