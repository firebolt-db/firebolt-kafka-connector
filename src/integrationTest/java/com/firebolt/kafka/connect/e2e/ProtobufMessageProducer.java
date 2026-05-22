package com.firebolt.kafka.connect.e2e;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DescriptorProtos;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Produces E2E test records as Protobuf DynamicMessages via Schema Registry.
 * Uses a runtime-built descriptor from the .proto schema rather than
 * compiled generated classes, keeping the build simple.
 */
@Slf4j
public class ProtobufMessageProducer implements MessageProducer {

    private final Producer<String, DynamicMessage> producer;
    private final Descriptors.Descriptor messageDescriptor;
    private final AtomicLong failedSendCount = new AtomicLong();

    public ProtobufMessageProducer(String bootstrapServers, String schemaRegistryUrl) {
        this.messageDescriptor = buildDescriptor();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32_000);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("auto.register.schemas", "true");
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void produce(String topicName, List<E2ETestRecord> records) {
        for (E2ETestRecord record : records) {
            DynamicMessage msg = toProtobuf(record);
            producer.send(new ProducerRecord<>(topicName, String.valueOf(record.getId()), msg),
                    (metadata, exception) -> {
                        if (exception != null) {
                            failedSendCount.incrementAndGet();
                            log.error("Failed to produce Protobuf record id={}: {}",
                                    record.getId(), exception.getMessage());
                        }
                    });
        }
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public long getFailedSendCount() {
        return failedSendCount.get();
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }

    private DynamicMessage toProtobuf(E2ETestRecord record) {
        return DynamicMessage.newBuilder(messageDescriptor)
                .setField(messageDescriptor.findFieldByName("id"), record.getId())
                .setField(messageDescriptor.findFieldByName("name"), record.getName())
                .setField(messageDescriptor.findFieldByName("value"), record.getValue())
                .setField(messageDescriptor.findFieldByName("timestamp"),
                        record.getTimestamp().toString())
                .build();
    }

    /**
     * Builds a Protobuf Descriptor at runtime matching the test_record.proto schema.
     * This avoids the need for protoc compilation in the build.
     */
    private static Descriptors.Descriptor buildDescriptor() {
        try {
            DescriptorProtos.FieldDescriptorProto idField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("id").setNumber(1)
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
                    .build();
            DescriptorProtos.FieldDescriptorProto nameField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("name").setNumber(2)
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                    .build();
            DescriptorProtos.FieldDescriptorProto valueField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("value").setNumber(3)
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE)
                    .build();
            DescriptorProtos.FieldDescriptorProto timestampField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("timestamp").setNumber(4)
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                    .build();

            DescriptorProtos.DescriptorProto msgDesc = DescriptorProtos.DescriptorProto.newBuilder()
                    .setName("TestRecord")
                    .addField(idField)
                    .addField(nameField)
                    .addField(valueField)
                    .addField(timestampField)
                    .build();

            DescriptorProtos.FileDescriptorProto fileDesc = DescriptorProtos.FileDescriptorProto.newBuilder()
                    .setName("test_record.proto")
                    .setSyntax("proto3")
                    .addMessageType(msgDesc)
                    .build();

            Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(
                    fileDesc, new Descriptors.FileDescriptor[]{});
            return fd.findMessageTypeByName("TestRecord");
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException("Failed to build Protobuf descriptor", e);
        }
    }
}
