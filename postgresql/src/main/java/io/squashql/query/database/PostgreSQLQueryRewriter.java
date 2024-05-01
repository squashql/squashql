package io.squashql.query.database;

import io.squashql.query.BinaryOperator;
import io.squashql.type.FunctionTypedField;
import io.squashql.type.TypedField;

class PostgreSQLQueryRewriter implements QueryRewriter {

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
  public boolean useAliasInHavingClause() {
    return false;
  }

  @Override
  public String functionExpression(FunctionTypedField ftf) {
    if ("current_date".contains(ftf.function())) {
      return "CURRENT_DATE";
    } else if (SqlFunctions.SUPPORTED_DATE_FUNCTIONS.contains(ftf.function())) {
      return String.format("extract(%s from %s)", ftf.function(), ftf.field().sqlExpression(this));
    } else {
      return QueryRewriter.super.functionExpression(ftf);
    }
  }

  @Override
  public String binaryOperation(BinaryOperator operator, String leftOperand, String rightOperand) {
    // NULLIF function takes two expressions and returns NULL if they are equal and if they are not equal it will return the first expression.
    return switch (operator) {
      case DIVIDE -> new StringBuilder()
              .append(leftOperand)
              .append(" ")
              .append(operator.infix)
              .append(" ")
              .append(String.format("NULLIF(%s,0)", rightOperand))
              .toString();
      default -> QueryRewriter.super.binaryOperation(operator, leftOperand, rightOperand);
    };
  }

  @Override
  public String arrayContains(TypedField field, Object value) {
    return String.format("%s=ANY(%s)", value, field.sqlExpression(this));
  }
}
