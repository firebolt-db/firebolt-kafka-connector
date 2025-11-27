package com.firebolt.kafka.connect.load.messagegenerator;

import com.firebolt.kafka.connect.load.LoadTestRecord;
import com.firebolt.kafka.connect.load.TestRecordFactory;

/**
 * A message generator that creates LoadTestRecord using the TestRecordFactory
 */
public class LoadTestRecordMessageGenerator implements MessageGenerator<LoadTestRecord> {

    private TestRecordFactory recordFactory;

    public LoadTestRecordMessageGenerator(TestRecordFactory recordFactory) {
        this.recordFactory = recordFactory;
    }

    @Override
    public LoadTestRecord nextMessage(int messageSequenceId) {
        return recordFactory.aValidRecord();
    }
}
