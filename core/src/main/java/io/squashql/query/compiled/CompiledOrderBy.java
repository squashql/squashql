package io.squashql.query.compiled;

import io.squashql.query.Functions;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.dto.ExplicitOrderDto;
import io.squashql.query.dto.OrderDto;
import io.squashql.query.dto.SimpleOrderDto;
import io.squashql.type.TypedField;

import java.util.Collections;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;

public record CompiledOrderBy(TypedField field, OrderDto orderDto) {

  public String sqlExpression(QueryRewriter queryRewriter) {
    StringJoiner joiner = new StringJoiner(" ");
    if (this.orderDto instanceof SimpleOrderDto simpleOrder) {
      String expression = this.field.sqlExpression(queryRewriter); // TODO todo-mde should be queryRewriter.orderBy(field) because some db supports using the alias, others not.
      joiner.add(expression).add(simpleOrder.order.name().toLowerCase());
      if (((SimpleOrderDto) orderDto).nullsOrder != null) {
        joiner.add("nulls").add(((SimpleOrderDto) orderDto).nullsOrder.name().toLowerCase());
      }
      return joiner.toString();
    } else if (this.orderDto instanceof ExplicitOrderDto explicitOrder) {
      AtomicInteger i = new AtomicInteger(0);
      joiner.add("case");
      CompiledCriteria isNull = new CompiledCriteria(Functions.isNull(), null, this.field, null, null, Collections.emptyList());
      joiner.add("when").add(isNull.sqlExpression(queryRewriter)).add("then").add(String.valueOf(i.incrementAndGet()));
      explicitOrder.explicit.forEach(element -> {
        CompiledCriteria cc = new CompiledCriteria(Functions.eq(element), null, this.field, null, null, Collections.emptyList());
        joiner.add("when").add(cc.sqlExpression(queryRewriter)).add("then").add(String.valueOf(i.incrementAndGet()));
      });
      joiner.add("else").add(String.valueOf(i.incrementAndGet()));
      joiner.add("end");
      return joiner.toString();
    } else {
      throw new UnsupportedOperationException("Unsupported order class " + this.orderDto.getClass().getSimpleName());
    }
  }
}
