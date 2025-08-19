package com.firebolt.kafka.connect.load;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
@AllArgsConstructor
@NoArgsConstructor
public class LoadTestRecord {

    private Integer colInteger;
    private Long colBigint;

    @JsonSerialize(using = ToStringSerializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private BigDecimal colNumeric;
    private Float colReal;
    private Double colDoublePrecision;

    private Boolean colBoolean;

    private String colText;

    private LocalDateTime colTimestamp;

}
