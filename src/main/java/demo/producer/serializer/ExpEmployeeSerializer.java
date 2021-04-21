package demo.producer.serializer;

import demo.dto.ExpEmployee;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

/**
 * 自定义化producer 序列化器
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/4/20 23:55
 **/
public class ExpEmployeeSerializer implements Serializer<ExpEmployee> {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, ExpEmployee employee) {
        if(Objects.isNull(employee)){
            return null;
        }
        byte[] employeeCode ;
        byte[] employeeName;
        try{
            if(StringUtils.isNotEmpty(employee.getEmployeeCode())){
                employeeCode = employee.getEmployeeCode().getBytes(StandardCharsets.UTF_8);
            }else{
                employeeCode = new byte[0];
            }

            if(StringUtils.isNotEmpty(employee.getEmployeeName())){
                employeeName = employee.getEmployeeName().getBytes(StandardCharsets.UTF_8);
            }else{
                employeeName = new byte[0];
            }
            ByteBuffer buffer = ByteBuffer.allocate(4+4 + employeeCode.length  + employeeName.length);
            buffer.putInt(employeeCode.length);
            buffer.put(employeeCode);
            buffer.putInt(employeeName.length);
            buffer.put(employeeName);
            return buffer.array();
        }catch (Exception e){
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
