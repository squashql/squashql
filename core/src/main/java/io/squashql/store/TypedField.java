package io.squashql.store;

public record TypedField(String store, String name, Class<?> type) {
}
