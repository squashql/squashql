package io.squashql.query.dto;

import io.squashql.query.Field;

public record MetadataItem(Field field, String expression, Class<?> type) {
}
