package com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schema;

import com.firebolt.kafka.connect.SchemaKafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.converter.exception.ColumnConversionFailedException;
import com.firebolt.kafka.connect.ingestion.binary.parquet.AbstractBinaryColumnTypeConverter;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;

/**
 * Schema-based adapter converting values to parquet/avro date logical type (days since epoch).
 */
public class SchemaDateBinaryColumnDataTypeConverter extends AbstractBinaryColumnTypeConverter<SchemaKafkaMessageColumnValue, Integer> {

    @Override
    public Integer toParquetValue(SchemaKafkaMessageColumnValue schemaKafkaMessageColumnValue, TableSchema.Column fireboltColumn) throws ColumnConversionFailedException {
        Object value = schemaKafkaMessageColumnValue.getValue();

        if (value instanceof Date) {
            Date date = (Date) value;
            LocalDate localDate = date.toInstant().atZone(ZoneOffset.UTC).toLocalDate();
            return (int) localDate.toEpochDay();
        }

        if (value instanceof Number) {
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


