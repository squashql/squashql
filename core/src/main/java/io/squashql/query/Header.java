package io.squashql.query;

public record Header(Field field, Class<?> type, boolean isMeasure) {
}
