package io.squashql.query.database;

import io.squashql.query.dto.CteColumnSetDto;

/**
 * Common table expression (for internal use only for the time being).
 * WITH <identifier> AS <subquery expression>.
 */
public record CTE(CteColumnSetDto cteColumnSetDto) {
}
