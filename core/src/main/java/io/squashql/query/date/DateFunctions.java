package io.squashql.query.date;

import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateFunctions {

  public static final Map<String, Pattern> DATE_PATTERNS = Map.of(
          "YEAR", Pattern.compile("__(YEAR)__(.*)__"),
          "QUARTER", Pattern.compile("__(QUARTER)__(.*)__"),
          "MONTH", Pattern.compile("__(MONTH)__(.*)__"));
  static final String YEAR_FORMAT = "__YEAR__%s__";
  static final String QUARTER_FORMAT = "__QUARTER__%s__";
  static final String MONTH_FORMAT = "__MONTH__%s__";

  public static String year(String column) {
    return String.format(YEAR_FORMAT, column);
  }

  public static String quarter(String column) {
    return String.format(QUARTER_FORMAT, column);
  }

  public static String month(String column) {
    return String.format(MONTH_FORMAT, column);
  }

//  public static String name(FunctionTypedField field) {
//    return String.format("__%s__%s__", field.function(), SqlUtils.expression(field.field()));
//  }

  public static Pair<String, String> extractFunctionAndFieldFromDateFunction(String str) {
    for (Map.Entry<String, Pattern> p : DATE_PATTERNS.entrySet()) {
      Matcher matcher = p.getValue().matcher(str);
      if (matcher.find()) {
        return Tuples.pair(p.getKey(), matcher.group(2));
      }
    }
    return Tuples.pair(null, str);
  }
}
