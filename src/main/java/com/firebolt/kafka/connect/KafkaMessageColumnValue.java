package com.firebolt.kafka.connect;

import java.util.Map;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import org.apache.kafka.connect.data.Schema;

@Data
@Getter
@Builder
public class KafkaMessageColumnValue {

    private Object value;

    private Schema.Type schemaType;

    /**
     * In case of an array we will keep here the type of each element in the array
     */
    private Schema.Type schemaSubType;

    private Map<String, String> schemaTypeParams;
}
