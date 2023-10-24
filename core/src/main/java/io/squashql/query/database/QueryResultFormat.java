package io.squashql.query.database;

import io.squashql.query.Measure;
import io.squashql.type.TypedField;

import java.util.List;

public record QueryResultFormat(List<TypedField> select, List<List<TypedField>> groupingSets, List<TypedField> rollup, List<Measure> measures) {
}
