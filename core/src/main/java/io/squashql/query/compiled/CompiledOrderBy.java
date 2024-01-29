package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;
import io.squashql.query.dto.OrderDto;
import io.squashql.type.TypedField;

public record CompiledOrderBy(TypedField field, OrderDto orderDto) {

  public String sqlExpression(QueryRewriter queryRewriter) {
    return null;
  }

}
