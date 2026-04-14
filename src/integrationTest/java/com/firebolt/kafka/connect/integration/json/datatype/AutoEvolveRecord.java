package com.firebolt.kafka.connect.integration.json.datatype;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * POJO used by AutoEvolveIntegrationTest.
 *
 * The Firebolt target table is created with only {@code id} and {@code name};
 * {@code extra} is absent from the table schema so that auto.evolve can be
 * exercised by issuing an ALTER TABLE ADD COLUMN for that field.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AutoEvolveRecord {
    private Integer id;
    private String name;
    private String extra;
}
