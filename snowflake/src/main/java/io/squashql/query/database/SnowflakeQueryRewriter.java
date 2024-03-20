package io.squashql.query.database;

import io.squashql.query.BinaryOperator;

class SnowflakeQueryRewriter implements QueryRewriter {

  @Override
  public String tableName(String table) {
    return SqlUtils.doubleQuoteEscape(table);
  }

  @Override
  public String cteName(String cteName) {
    return SqlUtils.doubleQuoteEscape(cteName);
  }

  @Override
  public String fieldName(String field) {
    return SqlUtils.doubleQuoteEscape(field);
  }

  @Override
  public String escapeAlias(String alias) {
    return SqlUtils.doubleQuoteEscape(alias);
  }

  @Override
  public boolean usePartialRollupSyntax() {
    return true;
  }

  @Override
  public String binaryOperation(BinaryOperator operator, String leftOperand, String rightOperand) {
    return switch (operator) {
      // https://docs.snowflake.com/en/sql-reference/functions/div0null
      case DIVIDE -> new StringBuilder()
              .append("DIV0NULL")
              .append("(")
              .append(leftOperand)
              .append(", ")
              .append(rightOperand)
              .append(")")
              .toString();
      default -> QueryRewriter.super.binaryOperation(operator, leftOperand, rightOperand);
    };
  }
}
