package io.squashql.query.compiled;

import io.squashql.query.database.QueryRewriter;
import io.squashql.type.TypedField;
import org.eclipse.collections.api.tuple.Pair;

import java.util.List;

public record CompiledVectorTupleAggMeasure(String alias,
                                            List<Pair<TypedField, String>> fieldToAggregateAndAggFunc,
                                            TypedField vectorAxis) implements CompiledMeasure {

  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    throw new IllegalStateException("Incorrect path of execution");
  }

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public <R> R accept(MeasureVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
