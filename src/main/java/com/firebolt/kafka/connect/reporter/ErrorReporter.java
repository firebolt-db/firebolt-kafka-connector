package com.firebolt.kafka.connect.reporter;

import org.apache.kafka.connect.sink.SinkRecord;

public interface ErrorReporter {
    void report(SinkRecord sinkRecord, Exception e);

    static ErrorReporter nullErrorReporter() {
        return (sinkRecord, e) -> { };
    }
}
