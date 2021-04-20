package demo.producer;

import demo.dto.ExpEmployee;
import demo.producer.serializer.ExpEmployeeSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/4/16 22:55
 **/
public class EmployeeSerializerProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = getProperties();

        KafkaProducer<String, ExpEmployee> producer = new KafkaProducer<>(props);

        ExpEmployee expEmployee = ExpEmployee.builder().employeeCode("EXP10001").employeeName("员工1").build();

        producer.send(new ProducerRecord<>("third", expEmployee),(metadata, exception) -> {
            if(exception == null){
                System.out.println("success " + metadata.topic() + ' '+ metadata.partition()+ ' '+ metadata.offset());
            }else{
                exception.printStackTrace();
            }
        }).get();

    }

    private static Properties getProperties() {
        Properties props = new Properties();
        // kafka broker集群
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"kafka1:9092,kafka2:9092,kafka3:9092");
        // 生产者acks参数
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        //批次大小
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //等待时间
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //RecordAccumulator 缓冲区大小
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        //key 序列化类
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        //value 序列化类
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ExpEmployeeSerializer.class.getName());
        return props;
    }
}
