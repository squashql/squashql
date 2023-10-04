package io.squashql.query;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.squashql.query.database.QueryRewriter;
import io.squashql.type.TypedField;
import java.util.function.Function;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface Field {

  String sqlExpression(Function<Field, TypedField> fieldProvider, QueryRewriter queryRewriter);

  String name();

  String alias();

  Field as(String alias);
}
