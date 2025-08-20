package com.firebolt.kafka.connect.load;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class FireboltSettings {

    private String jdbcUrl;

    /**
     * The clientId for the account used in the jdbc url
     */
    private String clientId;

    /**
     * The client secret corresponding to the clientId
     */
    private String clientSecret;
}
