package me.paulbares.store;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import me.paulbares.jackson.serializer.FieldTypeSerializer;

public record Field(String name, @JsonSerialize(using = FieldTypeSerializer.class) Class<?> type) {
}
