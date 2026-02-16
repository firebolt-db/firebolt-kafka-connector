package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter;

import com.firebolt.kafka.connect.TableSchema;
import com.firebolt.kafka.connect.datatype.FireboltColumnDataType;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schema.SchemaArrayDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schema.SchemaByteaDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schema.SchemaDateDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schema.SchemaDecimalDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schema.SchemaIntegerDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schema.SchemaRealDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schema.SchemaTimestampDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schema.SchemaTimestamptzDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schemaless.SchemalessBigIntDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schemaless.SchemalessBooleanDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schemaless.SchemalessDoubleDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schemaless.SchemalessJsonDataTypeConverter;
import com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter.schemaless.SchemalessTextDataTypeConverter;
import com.google.common.annotations.VisibleForTesting;

public class SchemaColumnTypeConverterFactory implements ColumnDataTypeConverterFactory {

    private SchemaIntegerDataTypeConverter integerDataTypeConverter ;
    private SchemaArrayDataTypeConverter arrayDataTypeConverter;
    private SchemaTimestampDataTypeConverter timestampDataTypeConverter;
    private SchemaTimestamptzDataTypeConverter timestamptzDataTypeConverter;
    private SchemaDateDataTypeConverter dateDataTypeConverter;
    private SchemaDecimalDataTypeConverter decimalDataTypeConverter;
    private SchemalessBigIntDataTypeConverter bigIntDataTypeConverter;
    private SchemaRealDataTypeConverter realDataTypeConverter;
    private SchemalessDoubleDataTypeConverter doubleDataTypeConverter;
    private SchemalessTextDataTypeConverter textDataTypeConverter;
    private SchemaByteaDataTypeConverter byteaDataTypeConverter;
    private SchemalessBooleanDataTypeConverter booleanDataTypeConverter;
    private SchemalessJsonDataTypeConverter jsonDataTypeConverter;

    public SchemaColumnTypeConverterFactory() {
        this(
                new SchemaIntegerDataTypeConverter(),
                new SchemaArrayDataTypeConverter(),
                new SchemaTimestampDataTypeConverter(),
                new SchemaTimestamptzDataTypeConverter(),
                new SchemaDecimalDataTypeConverter(),
                new SchemalessBigIntDataTypeConverter(),
                new SchemaRealDataTypeConverter(),
                new SchemalessDoubleDataTypeConverter(),
                new SchemalessTextDataTypeConverter(),
                new SchemaDateDataTypeConverter(),
                new SchemaByteaDataTypeConverter(),
                new SchemalessBooleanDataTypeConverter(),
                new SchemalessJsonDataTypeConverter()
        );
    }

    @VisibleForTesting
    SchemaColumnTypeConverterFactory(SchemaIntegerDataTypeConverter integerDataTypeConverter,
                                   SchemaArrayDataTypeConverter arrayDataTypeConverter,
                                   SchemaTimestampDataTypeConverter timestampDataTypeConverter,
                                   SchemaTimestamptzDataTypeConverter timestamptzDataTypeConverter,
                                   SchemaDecimalDataTypeConverter decimalDataTypeConverter,
                                   SchemalessBigIntDataTypeConverter bigIntDataTypeConverter,
                                   SchemaRealDataTypeConverter realDataTypeConverter,
                                   SchemalessDoubleDataTypeConverter doubleDataTypeConverter,
                                   SchemalessTextDataTypeConverter textDataTypeConverter,
                                   SchemaDateDataTypeConverter dateDataTypeConverter,
                                   SchemaByteaDataTypeConverter byteaDataTypeConverter,
                                   SchemalessBooleanDataTypeConverter booleanDataTypeConverter,
                                   SchemalessJsonDataTypeConverter jsonDataTypeConverter) {
        this.integerDataTypeConverter = integerDataTypeConverter;
        this.arrayDataTypeConverter = arrayDataTypeConverter;
        this.timestampDataTypeConverter = timestampDataTypeConverter;
        this.timestamptzDataTypeConverter = timestamptzDataTypeConverter;
        this.decimalDataTypeConverter = decimalDataTypeConverter;
        this.bigIntDataTypeConverter = bigIntDataTypeConverter;
        this.realDataTypeConverter = realDataTypeConverter;
        this.doubleDataTypeConverter = doubleDataTypeConverter;
        this.textDataTypeConverter = textDataTypeConverter;
        this.dateDataTypeConverter = dateDataTypeConverter;
        this.byteaDataTypeConverter = byteaDataTypeConverter;
        this.booleanDataTypeConverter = booleanDataTypeConverter;
        this.jsonDataTypeConverter = jsonDataTypeConverter;
    }

    public ColumnDataTypeConverter getConverter(TableSchema.Column fireboltTableColumn) {
        FireboltColumnDataType fireboltColumnDataType = FireboltColumnDataType.fromString(fireboltTableColumn.getDataType());

        switch (fireboltColumnDataType) {
            case INTEGER:
                return integerDataTypeConverter;
            case ARRAY:
                return arrayDataTypeConverter;
            case TIMESTAMP:
                return timestampDataTypeConverter;
            case TIMESTAMPTZ:
                return timestamptzDataTypeConverter;
            case DATE:
                return dateDataTypeConverter;
            case DECIMAL:
                return decimalDataTypeConverter;
            case BIGINT:
                return bigIntDataTypeConverter;
            case REAL:
                return realDataTypeConverter;
            case DOUBLE:
                return doubleDataTypeConverter;
            case TEXT:
                return textDataTypeConverter;
            case BYTEA:
                return byteaDataTypeConverter;
            case BOOLEAN:
                return booleanDataTypeConverter;
            case JSON:
                return jsonDataTypeConverter;
        }

        throw new IllegalArgumentException("Column type is not yet supported: " + fireboltTableColumn.getDataType() + " for column " + fireboltTableColumn.getName());
    }
}
