package me.paulbares.store;

import me.paulbares.query.dto.MetadataItem;

public record Field(String name, Class<?> type) implements MetadataItem {
}
