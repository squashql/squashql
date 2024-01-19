package io.squashql.query.compiled;

import io.squashql.type.NamedTypedField;

public record CompiledFieldAndAggFunc(NamedTypedField field, String aggFunc) {
}
