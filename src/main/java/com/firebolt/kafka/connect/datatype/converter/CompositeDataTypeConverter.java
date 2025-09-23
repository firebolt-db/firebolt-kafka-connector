package com.firebolt.kafka.connect.datatype.converter;

import com.firebolt.kafka.connect.KafkaMessageColumnValue;
import com.firebolt.kafka.connect.TableSchema;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import org.apache.kafka.connect.data.Schema;

public abstract class CompositeDataTypeConverter extends AbstractColumnTypeConverter {

}
