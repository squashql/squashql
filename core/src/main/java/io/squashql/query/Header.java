package io.squashql.query;

import io.squashql.store.Field;

public record Header(Field field, boolean isMeasure) {
}
