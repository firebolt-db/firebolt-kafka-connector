package com.firebolt.kafka.connect.load.messagegenerator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.IOException;
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
            if (value == null) {
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


    /**
     * Returns true if another message can be generated.
     */
    private boolean hasNext() {
        return currentRowIndex < rows.size();
    }

    private void loadCsv() {
        Path path = get(sourceCsvFile);
        try (BufferedReader reader = newBufferedReader(path)) {
            String line = reader.readLine();
            if (line == null) {
                return;
            }
            headers = parseCsvLine(line);

            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                rows.add(parseCsvLine(line));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read CSV file: " + sourceCsvFile, e);
        }
    }

    /**
     * Basic CSV line parser that supports quoted fields and commas inside quotes.
     */
    private List<String> parseCsvLine(String line) {
        List<String> result = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    // Escaped quote
                    current.append('"');
                    i++; // skip next quote
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                result.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        result.add(current.toString());
        return result;
    }

}
