package io.squashql.query.compiled;

import io.squashql.type.TypedField;

public record CompiledFieldAndAggFunc(TypedField field, String aggFunc) {
}
