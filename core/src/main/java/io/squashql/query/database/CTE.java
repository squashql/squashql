package io.squashql.query.database;

/**
 * Common table expression (for internal use only for the time being).
 * WITH <identifier> AS <subquery expression>.
 */
public record CTE(String identifier, String subqueryExpression) {
}
