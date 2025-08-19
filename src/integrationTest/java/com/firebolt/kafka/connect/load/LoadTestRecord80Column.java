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
public class LoadTestRecord80Column extends LoadTestRecord {

    // Additional Integer columns (9 more to make 10 total including inherited colInteger)
    private Integer colInteger2;
    private Integer colInteger3;
    private Integer colInteger4;
    private Integer colInteger5;
    private Integer colInteger6;
    private Integer colInteger7;
    private Integer colInteger8;
    private Integer colInteger9;
    private Integer colInteger10;

    // Additional Bigint columns (9 more to make 10 total including inherited colBigint)
    private Long colBigint2;
    private Long colBigint3;
    private Long colBigint4;
    private Long colBigint5;
    private Long colBigint6;
    private Long colBigint7;
    private Long colBigint8;
    private Long colBigint9;
    private Long colBigint10;

    // Additional Numeric columns (9 more to make 10 total including inherited colNumeric)
    
    @JsonSerialize(using = ToStringSerializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private BigDecimal colNumeric2;
    
    @JsonSerialize(using = ToStringSerializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private BigDecimal colNumeric3;
    
    @JsonSerialize(using = ToStringSerializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private BigDecimal colNumeric4;
    
    @JsonSerialize(using = ToStringSerializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private BigDecimal colNumeric5;
    
    @JsonSerialize(using = ToStringSerializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private BigDecimal colNumeric6;
    
    @JsonSerialize(using = ToStringSerializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private BigDecimal colNumeric7;
    
    @JsonSerialize(using = ToStringSerializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private BigDecimal colNumeric8;
    
    @JsonSerialize(using = ToStringSerializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private BigDecimal colNumeric9;
    
    @JsonSerialize(using = ToStringSerializer.class)
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private BigDecimal colNumeric10;

    // Additional Real columns (9 more to make 10 total including inherited colReal)
    private Float colReal2;
    private Float colReal3;
    private Float colReal4;
    private Float colReal5;
    private Float colReal6;
    private Float colReal7;
    private Float colReal8;
    private Float colReal9;
    private Float colReal10;

    // Additional Double Precision columns (9 more to make 10 total including inherited colDoublePrecision)
    private Double colDoublePrecision2;
    private Double colDoublePrecision3;
    private Double colDoublePrecision4;
    private Double colDoublePrecision5;
    private Double colDoublePrecision6;
    private Double colDoublePrecision7;
    private Double colDoublePrecision8;
    private Double colDoublePrecision9;
    private Double colDoublePrecision10;

    // Additional Boolean columns (9 more to make 10 total including inherited colBoolean)
    private Boolean colBoolean2;
    private Boolean colBoolean3;
    private Boolean colBoolean4;
    private Boolean colBoolean5;
    private Boolean colBoolean6;
    private Boolean colBoolean7;
    private Boolean colBoolean8;
    private Boolean colBoolean9;
    private Boolean colBoolean10;

    // Additional Text columns (9 more to make 10 total including inherited colText)
    private String colText2;
    private String colText3;
    private String colText4;
    private String colText5;
    private String colText6;
    private String colText7;
    private String colText8;
    private String colText9;
    private String colText10;

    // Additional Timestamp columns (9 more to make 10 total including inherited colTimestamp)
    private LocalDateTime colTimestamp2;
    private LocalDateTime colTimestamp3;
    private LocalDateTime colTimestamp4;
    private LocalDateTime colTimestamp5;
    private LocalDateTime colTimestamp6;
    private LocalDateTime colTimestamp7;
    private LocalDateTime colTimestamp8;
    private LocalDateTime colTimestamp9;
    private LocalDateTime colTimestamp10;
}
