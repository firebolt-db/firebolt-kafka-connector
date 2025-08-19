package com.firebolt.kafka.connect.load;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;

@Builder
@Getter
public class LoadTestRunResult {

    /**
     * True if the run completed successfully, false otherwise
     */
    private boolean completedSuccessfully;

    /**
     * How many rows per second were inserted into firebolt. (e.g; 1250 would mean that we would have inserted 1250 rows / second into firebolt)
     */
    private long fireboltIngestionRate;

    /**
     * How long did it take for the ingestion in firebolt
     */
    private Duration fireboltTotalIngestionDuration;

    /**
     * Will return some details from query history since we will lose the query history when the engine stops
     */
    private List<String> queryHistoryDetails;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LoadTestRunResult[");
        sb.append("success=").append(completedSuccessfully);
        sb.append(", ingestionRate=").append(fireboltIngestionRate).append(" rows/sec");
        
        if (fireboltTotalIngestionDuration != null) {
            sb.append(", duration=").append(formatDuration(fireboltTotalIngestionDuration));
        }
        
        if (queryHistoryDetails != null && !queryHistoryDetails.isEmpty()) {
            sb.append(", queryHistoryEntries=").append(queryHistoryDetails.size()).append("\n");

            if (!CollectionUtils.isEmpty(queryHistoryDetails)) {
                queryHistoryDetails.stream().forEach(query -> sb.append(query).append("\n"));
            }
        }
        
        sb.append("]");


        return sb.toString();
    }

    private String formatDuration(Duration duration) {
        long seconds = duration.getSeconds();
        if (seconds < 60) {
            return seconds + "s";
        } else if (seconds < 3600) {
            long minutes = seconds / 60;
            long remainingSeconds = seconds % 60;
            return minutes + "m " + remainingSeconds + "s";
        } else {
            long hours = seconds / 3600;
            long remainingMinutes = (seconds % 3600) / 60;
            long remainingSeconds = seconds % 60;
            return hours + "h " + remainingMinutes + "m " + remainingSeconds + "s";
        }
    }
}
