package io.squashql.query;

import io.squashql.store.FieldWithStore;

public record Header(FieldWithStore field, boolean isMeasure) {
}
