package io.squashql.query.database;

import io.squashql.query.compiled.CompiledMeasure;

import java.util.List;

public record DatabaseQuery(QueryScope scope, List<CompiledMeasure> measures) {
}
