package com.firebolt.kafka.connect;

import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class JdbcConfig {

    private String jdbcConnectionUrl;

    private Optional<String> clientId;

    private Optional<String> clientSecret;

}
