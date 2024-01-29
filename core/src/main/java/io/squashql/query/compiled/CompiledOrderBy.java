package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;
import io.squashql.query.dto.ExplicitOrderDto;
import io.squashql.query.dto.OrderDto;
import io.squashql.query.dto.SimpleOrderDto;
import io.squashql.type.TypedField;

import java.util.StringJoiner;

public record CompiledOrderBy(TypedField field, OrderDto orderDto) {

  public String sqlExpression(final QueryRewriter queryRewriter) {
    final String expression = field.sqlExpression(queryRewriter); // todo-mde should be queryRewriter.orderBy(field) because some db supports using the alias, others not.
    final StringJoiner joiner = new StringJoiner(" ");
    if (this.orderDto instanceof SimpleOrderDto simpleOrder) {
      joiner.add(expression).add(simpleOrder.order.name());
      return joiner.toString();
    } else if (this.orderDto instanceof ExplicitOrderDto explicitOrder) {
      joiner.add("CASE");
      explicitOrder.explicit.forEach(element -> {
        joiner.add(expression).add("=").add(element.toString());
      });
      joiner.add("END");
    } else {
      throw new UnsupportedOperationException("Unsupported order class " + orderDto.getClass().getSimpleName());
    }
    return null;
  }

}
