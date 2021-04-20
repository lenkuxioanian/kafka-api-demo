package demo.dto;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * TODO
 *
 * @author Administrator
 * @version 1.0
 * @date 2021/4/20 23:21
 **/
@Data
@AllArgsConstructor
@Builder
@NoArgsConstructor
public class ExpEmployee {
    private String employeeCode;
    private String employeeName;
}
