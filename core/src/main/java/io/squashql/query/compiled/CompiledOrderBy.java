package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.ExplicitOrderDto;
import io.squashql.query.dto.OrderDto;
import io.squashql.query.dto.SimpleOrderDto;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;

import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.squashql.query.agg.AggregationFunction.GROUPING;

public record CompiledOrderBy(TypedField field, OrderDto orderDto, boolean withTotal) {

  public String sqlExpression(final QueryRewriter queryRewriter) {
    final String expression = this.field.sqlExpression(queryRewriter); // todo-mde should be queryRewriter.orderBy(field) because some db supports using the alias, others not.
    final StringJoiner joiner = new StringJoiner(" ");
    if (withTotal) {
      //todo maybe cache
      final CompiledMeasure rollUpMeasure = new CompiledAggregatedMeasure(
              SqlUtils.groupingAlias(expression.replace(".", "_")),
              this.field, GROUPING, null, false);
      joiner.add(rollUpMeasure.sqlExpression(queryRewriter, false)).add("DESC,");
    }
    if (this.orderDto instanceof SimpleOrderDto simpleOrder) {
      joiner.add(expression).add(simpleOrder.order.name());
      return joiner.toString();
    } else if (this.orderDto instanceof ExplicitOrderDto explicitOrder) {
      final Function<Object, String> sqlMapper = field instanceof TableTypedField ? SQLTranslator.getQuoteFn(field, queryRewriter) : String::valueOf; // FIXME dirty workaround
      final AtomicInteger i = new AtomicInteger(0);
      joiner.add("CASE");
      explicitOrder.explicit.forEach(element -> {
        joiner.add("when").add(expression).add("=").add(sqlMapper.apply(element)).add("then").add(String.valueOf(i.incrementAndGet()));
      });
      joiner.add("else").add(String.valueOf(i.incrementAndGet()));
      joiner.add("END");
      return joiner.toString();
    } else {
      throw new UnsupportedOperationException("Unsupported order class " + orderDto.getClass().getSimpleName());
    }
  }

}
