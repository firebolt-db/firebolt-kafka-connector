package com.firebolt.kafka.connect.convert;

import com.firebolt.kafka.connect.AbstractFireboltRecord;
import com.firebolt.kafka.connect.FireboltRecord;
import com.firebolt.kafka.connect.SinkConfig;
import com.firebolt.kafka.connect.convert.exception.RecordConversionException;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Factory that creates and manages RecordConverter instances.
 * Automatically selects the appropriate converter based on the SinkRecord characteristics.
 */
@Slf4j
public class RecordConverterFactory {

    private final List<RecordConverter> converters;

    /**
     * Creates a converter factory with the given configuration.
     *
     * @param config the sink configuration
     */
    public RecordConverterFactory(SinkConfig config) {
        this.converters = new ArrayList<>();

        // Register converters in order of preference
        converters.add(new SchemaBasedRecordConverter(config));
        converters.add(new SchemalessBasedRecordConverter(config));

        log.info("Initialized RecordConverterFactory with {} converters", converters.size());
        for (RecordConverter converter : converters) {
            log.debug("Registered converter: {}", converter.getDescription());
        }
    }

    /**
     * Converts a SinkRecord using the appropriate converter.
     *
     * @param record the SinkRecord to convert
     * @return the converted FireboltRecord
     * @throws RecordConversionException if conversion fails
     */
    public AbstractFireboltRecord convert(SinkRecord record)
            throws RecordConversionException {

        RecordConverter selectedConverter = selectConverter(record);
        return selectedConverter.convert(record);
    }

    /**
     * Selects the appropriate converter for the given record.
     *
     * @param record the SinkRecord to analyze
     * @return the appropriate RecordConverter
     */
    private RecordConverter selectConverter(SinkRecord record) {
        // Try each converter in order
        for (RecordConverter converter : converters) {
            if (converter.canHandle(record)) {
                return converter;
            }
        }

        return null;
    }

}