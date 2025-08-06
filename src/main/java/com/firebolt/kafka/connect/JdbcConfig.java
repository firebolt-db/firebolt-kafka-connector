package com.firebolt.kafka.connect;

import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class JdbcConfig {

    private String jdbcConnectionUrl;

    private Optional<String> clientId;

    private Optional<String> clientSecret;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("JdbcConfig{");
        sb.append("jdbcConnectionUrl='").append(jdbcConnectionUrl).append('\'');
        
        if (clientId.isPresent() && clientId.get() != null) {
            String id = clientId.get();
            if (id.length() > 4) {
                sb.append(", clientId='***").append(id.substring(id.length() - 4)).append('\'');
            } else {
                sb.append(", clientId='***").append(id).append('\'');
            }
        }
        
        if (clientSecret.isPresent() && clientSecret.get() != null && !clientSecret.get().isEmpty()) {
            sb.append(", clientSecret='***'");
        }
        
        sb.append('}');
        return sb.toString();
    }
}
