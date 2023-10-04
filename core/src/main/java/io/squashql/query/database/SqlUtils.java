package io.squashql.query.database;

import io.squashql.query.TableField;
import io.squashql.type.FunctionTypedField;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;

public class SqlUtils {

  public static String backtickEscape(String column) {
    return "`" + column + "`";
  }

  public static String doubleQuoteEscape(String column) {
    return "\"" + column + "\"";
  }

  public static String appendAlias(String sql, QueryRewriter queryRewriter, String alias) {
    return sql + " as " + queryRewriter.measureAlias(alias);
  }

  public static String getFieldFullName(String store, String name) {
    return store == null ? name : store + '.' + name;
  }

  public static String getFieldFullName(TableTypedField field) {
    return field.getStore() == null ? field.getName() : field.getStore() + '.' + field.getName();
  }

  public static String getFieldFullName(TableField field) {
    return field.tableName == null ? field.name() : field.tableName + '.' + field.name();
  }

  public static String expression(TypedField f) {
    if (f instanceof TableTypedField ttf) {
      return getFieldFullName(ttf);
    } else if (f instanceof FunctionTypedField ftf) {
      return singleOperandFunctionName(ftf.function(), getFieldFullName(ftf.field()));
    } else {
      throw new IllegalArgumentException(f.getClass().getName());
    }
  }

  public static String singleOperandFunctionName(String function, String operand) {
    return function + "(" + operand + ")";
  }

  public static String groupingExpression(QueryRewriter qr, TypedField tf) {
    return String.format("grouping(%s)", qr.select(tf));
  }
}
