package com.firebolt.kafka.connect.ingestion.binary.parquet;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessArrayColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessBigIntColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessIntegerColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessTimestampColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessTimestamptzColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessRealColumnDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.binary.parquet.datatype.converter.schemless.SchemalessDecimalColumnDataTypeConverter;
import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;

public class SchemalessColumnDataTypeConverterFactory implements ColumnDataTypeConverterFactory {

    private SchemalessIntegerColumnDataTypeConverter integerDataTypeConverter;
    private SchemalessBigIntColumnDataTypeConverter bigIntDataTypeConverter;
    private SchemalessTimestampColumnDataTypeConverter timestampDataTypeConverter;
    private SchemalessTimestamptzColumnDataTypeConverter timestamptzDataTypeConverter;
    private SchemalessRealColumnDataTypeConverter realDataTypeConverter;
    private SchemalessDecimalColumnDataTypeConverter decimalDataTypeConverter;
    private SchemalessArrayColumnDataTypeConverter arrayColumnDataTypeConverter;

    public SchemalessColumnDataTypeConverterFactory() {
        this(
                new SchemalessIntegerColumnDataTypeConverter(),
                new SchemalessBigIntColumnDataTypeConverter(),
                new SchemalessTimestampColumnDataTypeConverter(),
                new SchemalessTimestamptzColumnDataTypeConverter(),
                new SchemalessRealColumnDataTypeConverter(),
                new SchemalessDecimalColumnDataTypeConverter(),
                new SchemalessArrayColumnDataTypeConverter()
        );
    }

    @VisibleForTesting
    SchemalessColumnDataTypeConverterFactory(SchemalessIntegerColumnDataTypeConverter integerDataTypeConverter,
                                             SchemalessBigIntColumnDataTypeConverter bigIntDataTypeConverter,
                                             SchemalessTimestampColumnDataTypeConverter timestampDataTypeConverter,
                                             SchemalessTimestamptzColumnDataTypeConverter timestamptzDataTypeConverter,
                                             SchemalessRealColumnDataTypeConverter realDataTypeConverter,
                                             SchemalessDecimalColumnDataTypeConverter decimalDataTypeConverter,
                                             SchemalessArrayColumnDataTypeConverter arrayColumnDataTypeConverter) {
        this.integerDataTypeConverter = integerDataTypeConverter;
        this.bigIntDataTypeConverter = bigIntDataTypeConverter;
        this.timestampDataTypeConverter = timestampDataTypeConverter;
        this.timestamptzDataTypeConverter = timestamptzDataTypeConverter;
        this.realDataTypeConverter = realDataTypeConverter;
        this.decimalDataTypeConverter = decimalDataTypeConverter;
        this.arrayColumnDataTypeConverter = arrayColumnDataTypeConverter;

        // need to populate the array column data type with the values that it can convert
        arrayColumnDataTypeConverter.addConverter(Integer.class, integerDataTypeConverter);
        arrayColumnDataTypeConverter.addConverter(Long.class, bigIntDataTypeConverter);
        arrayColumnDataTypeConverter.addConverter(Float.class, realDataTypeConverter);
        arrayColumnDataTypeConverter.addConverter(ByteBuffer.class, decimalDataTypeConverter);
        arrayColumnDataTypeConverter.setTimestampConverter(timestampDataTypeConverter);
        arrayColumnDataTypeConverter.setTimestamptzConverter(timestamptzDataTypeConverter);
    }

    @Override
    public ColumnDataTypeConverter getConverter(TableSchema.Column fireboltTableColumn) {
        FireboltColumnDataType fireboltColumnDataType = FireboltColumnDataType.fromString(fireboltTableColumn.getDataType());

        switch (fireboltColumnDataType) {
            case INTEGER:
                return integerDataTypeConverter;
            case BIGINT:
                return bigIntDataTypeConverter;
            case TIMESTAMP:
                return timestampDataTypeConverter;
            case TIMESTAMPTZ:
                return timestamptzDataTypeConverter;
            case REAL:
                return realDataTypeConverter;
            case DECIMAL:
                return decimalDataTypeConverter;
            case ARRAY:
                return arrayColumnDataTypeConverter;
        }

        throw new IllegalArgumentException("Column type is not yet supported: " + fireboltTableColumn.getDataType() + " for column " + fireboltTableColumn.getName());
    }
}
