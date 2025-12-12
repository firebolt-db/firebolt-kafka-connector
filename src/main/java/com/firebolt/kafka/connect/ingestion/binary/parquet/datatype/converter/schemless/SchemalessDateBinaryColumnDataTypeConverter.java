package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless;

import com.firebolt.kafka.connect.SchemalessKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractBinaryBinaryColumnTypeConverter;
import java.time.LocalDate;

/**
 * Converts a schemaless kafka message column value to days since epoch (Integer) for parquet/avro date logical type.
 */
public class SchemalessDateBinaryColumnDataTypeConverter extends AbstractBinaryBinaryColumnTypeConverter<SchemalessKafkaMessageColumnValue, Integer> {

    @Override
    public Integer toParquetValue(SchemalessKafkaMessageColumnValue schemalessKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        Object value = schemalessKafkaMessageColumnValue.getValue();

        if (value instanceof Number) {
            // Treat numbers as days since epoch
            long days = ((Number) value).longValue();
            return (int) days;
        }

        if (value instanceof String) {
            String s = (String) value;
            if (isIsoLocalDate(s)) {
                long days = LocalDate.parse(s).toEpochDay();
                return (int) days;
            }
            throw new ColumnConversionFailedException(fireboltColumn.getName(), fireboltColumn.getDataType(),
                    "String value cannot be converted to a date as it is not an ISO local date (yyyy-MM-dd)");
        }

        throw aColumnConversionFailedException(fireboltColumn, value);
    }

    @Override
    public Class<Integer> getConvertedType() {
        return Integer.class;
    }
}


