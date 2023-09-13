package io.squashql.type;

public record TableTypedField(String store, String name, Class<?> type) implements TypedField {
}
