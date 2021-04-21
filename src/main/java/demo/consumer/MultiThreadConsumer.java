package demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 多线程消费者
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/4/16 23:56
 **/
public class MultiThreadConsumer {

    public static void main(String[] args) {

        Properties props = getProperties();

        for(int i = 0; i< 2; i++){
            new KafkaThreadConsumer(props, Arrays.asList("first", "second")).start();
        }

    }

    private static Properties getProperties() {
        Properties props = new Properties();
        //kafka broker集群
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092,kafka2:9092,kafka3:9092");
        //消费者组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-transaction-4");
        //开启自动提交offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //自动提交offset间隔
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
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

    public static class KafkaThreadConsumer extends Thread{

        private KafkaConsumer<String, String> consumer;

        KafkaThreadConsumer(Properties props, List<String> topics){
            this.consumer = new KafkaConsumer<>(props);
            this.consumer.subscribe(topics);
        }

        @Override
        public void run(){
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecord = consumer.poll(2000);
                    for (ConsumerRecord<String, String> record : consumerRecord) {
                        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                    }
                }
            }catch (Exception e){
                e.getStackTrace();
            }finally {
                consumer.close();
            }
        }
    }
}
