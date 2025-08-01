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

    private Map<String, String> schemaTypeParams;
}
