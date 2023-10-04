package io.squashql.query.database;

import io.squashql.query.TableField;
import io.squashql.type.FunctionTypedField;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlUtils {

  static final Pattern GROUPING_PATTERN = Pattern.compile("___grouping___(.*)___");

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
    return field.store() == null ? field.name() : field.store() + '.' + field.name();
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

  /**
   * See {@link #groupingAlias(String)}.
   */
  public static String extractFieldFromGroupingAlias(String str) {
    Matcher matcher = GROUPING_PATTERN.matcher(str);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
  }


  /**
   * Returns the name of the column used for grouping(). If it is modified, please modify also
   * {@link SqlUtils#GROUPING_PATTERN}.
   */
  public static String groupingAlias(String column) {
    return String.format("___grouping___%s___", column);
  }
}
