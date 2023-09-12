package io.squashql.store;

import io.squashql.type.TypedField;

import java.util.List;

public record Store(String name, List<TypedField> fields) {
}
