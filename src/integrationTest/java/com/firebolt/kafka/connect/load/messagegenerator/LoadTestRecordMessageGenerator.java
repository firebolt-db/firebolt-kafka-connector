package com.firebolt.kafka.connect.load.messagegenerator;

import com.firebolt.kafka.connect.load.LoadTestRecord;
import com.firebolt.kafka.connect.load.TestRecordFactory;
import com.firebolt.kafka.connect.load.verifier.LoadTestRecordFireboltTableVerifier;

/**
 * A message generator that creates LoadTestRecord using the TestRecordFactory
 */
public class LoadTestRecordMessageGenerator implements MessageGenerator<LoadTestRecord> {

    private TestRecordFactory recordFactory;
    private LoadTestRecordFireboltTableVerifier loadTestRecordFireboltTableVerifier;

    // how many messages we are planning to produce
    private int messageCount;

    public LoadTestRecordMessageGenerator(TestRecordFactory recordFactory, LoadTestRecordFireboltTableVerifier loadTestRecordFireboltTableVerifier, int messageCount) {
        this.recordFactory = recordFactory;
        this.loadTestRecordFireboltTableVerifier = loadTestRecordFireboltTableVerifier;
        this.messageCount = messageCount;
    }

    @Override
    public LoadTestRecord nextMessage(int messageSequenceId) {
        LoadTestRecord loadTestRecord = recordFactory.aValidRecord(messageSequenceId);

        // by default add the first and last message to the verification
        if (messageSequenceId == 1 || messageSequenceId == messageCount) {
            // add the first record
            loadTestRecordFireboltTableVerifier.addRecordToVerification(loadTestRecord);
        } else if (messageSequenceId % 1000 == 0) { // if not the first or last one, then add every 1000 to be verified
            loadTestRecordFireboltTableVerifier.addRecordToVerification(loadTestRecord);
        }

        return loadTestRecord;
    }
}
