package io.squashql.query.database;

import io.squashql.query.Field;
import io.squashql.query.FunctionField;
import io.squashql.query.TableField;
import io.squashql.type.FunctionTypedField;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.squashql.query.agg.AggregationFunction.GROUPING;

public class SqlUtils {

  static final Pattern GROUPING_PATTERN = Pattern.compile(String.format("___%s___(.*)___", GROUPING));

  public static String backtickEscape(String column) {
    return "`" + column + "`";
  }

  public static String doubleQuoteEscape(String column) {
    return "\"" + column + "\"";
  }

  public static String appendAlias(String sql, QueryRewriter queryRewriter, String alias) {
    return sql + " as " + queryRewriter.escapeAlias(alias);
  }

  public static String getFieldFullName(String store, String name) {
    return store == null ? name : store + '.' + name;
  }

  public static String getFieldFullName(TableTypedField field) {
    return field.store() == null ? field.name() : field.store() + '.' + field.name();
  }

  /**
   * This expression is different than {@link TypedField#sqlExpression(QueryRewriter)} because it is valid only for SquashQL
   * and not for the underlying DB.
   */
  public static String squashqlExpression(TypedField f) {
    if (f.alias() != null) {
      return f.alias();
    }

    if (f instanceof TableTypedField ttf) {
      return getFieldFullName(ttf);
    } else if (f instanceof FunctionTypedField ftf) {
      return singleOperandFunctionName(ftf.function(), squashqlExpression(ftf.field()));
    } else {
      throw new IllegalArgumentException(f.getClass().getName());
    }
  }

  public static String squashqlExpression(Field f) {
    if (f.alias() != null) {
      return f.alias();
    }

    if (f instanceof TableField tf) {
      return getFieldFullName(tf.tableName, tf.fieldName);
    } else if (f instanceof FunctionField ftf) {
      return singleOperandFunctionName(ftf.function, squashqlExpression(ftf.field));
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
    return String.format("___%s___%s___", GROUPING, column);
  }

  public static String columnAlias(String column) {
    return String.format("___%s___%s___", "alias", column);
  }

  public static String escapeSingleQuote(String s, String escapeCharacter) {
    return s.replace("'", escapeCharacter);
  }
}
