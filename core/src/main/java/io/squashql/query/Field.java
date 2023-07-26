package io.squashql.query;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.squashql.query.database.QueryRewriter;
import io.squashql.store.TypedField;

import java.util.function.Function;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface Field {

  String sqlExpression(Function<String, TypedField> fieldProvider, QueryRewriter queryRewriter);
}
