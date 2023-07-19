package io.squashql.store;

import java.util.List;

public record Store(String name, List<TypedField> fields) {
}
