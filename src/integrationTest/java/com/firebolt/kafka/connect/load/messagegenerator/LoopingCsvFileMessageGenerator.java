package com.firebolt.kafka.connect.load.messagegenerator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

import static java.nio.file.Files.*;
import static java.nio.file.Paths.*;

/**
 * Generates kafka messages by reading the messages from a CSV file.
 * If the file contains only 10k rows, but we need to generate 100k messages we will loop the file 10 times.
 */
@Slf4j
public class LoopingCsvFileMessageGenerator implements MessageGenerator<String> {

    private String sourceCsvFile;
    private List<String> headers;
    private java.util.List<List<String>> rows;
    private int currentRowIndex;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public LoopingCsvFileMessageGenerator(String sourceCsvFile) {
        this.sourceCsvFile = sourceCsvFile;
        this.headers = new java.util.ArrayList<>();
        this.rows = new java.util.ArrayList<>();
        this.currentRowIndex = 0;
        loadCsv();
    }

    @Override
    public String nextMessage(int messageSequenceId) {
        if (currentRowIndex >= rows.size()) {
            log.info("Resetting the csv index to start from first line. Original row index was: {}, not it is 0", currentRowIndex);
            currentRowIndex = 0;
        }

        List<String> values = rows.get(currentRowIndex);
        currentRowIndex++;

        ObjectNode node = objectMapper.createObjectNode();
        int count = headers.size();
        for (int i = 0; i < count; i++) {
            String key = headers.get(i);
            String value = i < values.size() ? values.get(i) : null;
            if (value == null || value.equals("NULL")) {
                node.putNull(key);
            } else {
                node.put(key, value);
            }
        }
        try {
            return objectMapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize JSON", e);
        }
    }


    // no hasNext() needed; generator loops automatically when reaching the end

    private void loadCsv() {
        Path path = get(sourceCsvFile);
        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setIgnoreSurroundingSpaces(true)
                .setTrim(true)
                .setQuote('"')
                .build();
        try (Reader reader = newBufferedReader(path);
             CSVParser parser = new CSVParser(reader, format)) {
            List<CSVRecord> records = parser.getRecords();
            if (records.isEmpty()) {
                return;
            }
            // first record is header
            CSVRecord headerRecord = records.get(0);
            headers = new java.util.ArrayList<>(headerRecord.size());
            for (String h : headerRecord) {
                headers.add(h);
            }
            // remaining records are data
            for (int i = 1; i < records.size(); i++) {
                CSVRecord r = records.get(i);
                List<String> values = new java.util.ArrayList<>(r.size());
                for (String v : r) {
                    values.add(v);
                }
                rows.add(values);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read CSV file: " + sourceCsvFile, e);
        }
    }

    // Legacy single-line parser removed in favor of Apache Commons CSV which supports multi-line records.

}
