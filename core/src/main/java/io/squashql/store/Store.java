package io.squashql.store;

import io.squashql.type.TableTypedField;

import java.util.List;

public record Store(String name, List<TableTypedField> fields) {
}
