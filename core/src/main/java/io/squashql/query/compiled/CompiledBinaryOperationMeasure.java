package io.squashql.query.compiled;

import io.squashql.query.BinaryOperator;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.database.SqlUtils;
import lombok.NonNull;

public record CompiledBinaryOperationMeasure(@NonNull String alias, @NonNull BinaryOperator operator,
                                             @NonNull CompiledMeasure leftOperand, @NonNull CompiledMeasure rightOperand) implements CompiledMeasure {
  @Override
  public String sqlExpression(QueryRewriter queryRewriter, boolean withAlias) {
    String sql = new StringBuilder()
            .append("(")
            .append(this.leftOperand.sqlExpression(queryRewriter, false))
            .append(this.operator.infix)
            .append(this.rightOperand.sqlExpression(queryRewriter, false))
            .append(")")
            .toString();
    return withAlias ? SqlUtils.appendAlias(sql, queryRewriter, this.alias) : sql;
  }

}
