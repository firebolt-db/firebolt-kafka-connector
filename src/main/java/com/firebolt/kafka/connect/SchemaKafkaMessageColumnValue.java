package com.firebolt.kafka.connect;

import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.connect.data.Schema;

@Data
@Getter
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class SchemaKafkaMessageColumnValue extends SchemalessKafkaMessageColumnValue {

    private Schema.Type schemaType;

    /**
     * In case of an array we will keep here the type of each element in the array
     */
    private Schema.Type schemaSubType;

    private Map<String, String> schemaTypeParams;

}
