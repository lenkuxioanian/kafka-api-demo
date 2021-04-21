package demo.consumer.deserializer;

import demo.dto.ExpEmployee;
import org.apache.commons.lang.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/4/16 23:56
 **/
public class ExpEmployeeDeserializer implements Deserializer<ExpEmployee> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public ExpEmployee deserialize(String s, byte[] bytes) {
        if (Objects.isNull(bytes)) {
            return null;
        }
        if (bytes.length < 8) {
            throw new SerializationException("size is error");
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        int employeeCodeLength;
        int employeeNameLength;
        String employeeCode = null;
        String employeeName = null;

        employeeCodeLength = byteBuffer.getInt();
        byte[] employeeCodeByte = new byte[employeeCodeLength];
        byteBuffer.get(employeeCodeByte);

        employeeNameLength = byteBuffer.getInt();
        byte[] employeeNameByte = new byte[employeeNameLength];
        byteBuffer.get(employeeNameByte);

        try {
            employeeCode = new String(employeeCodeByte, StandardCharsets.UTF_8);
            employeeName = new String(employeeNameByte, StandardCharsets.UTF_8);
        } catch (Exception e) {
            e.getStackTrace();
        }
        return new ExpEmployee(employeeCode,employeeName);
    }

    @Override
    public void close() {

    }
}
