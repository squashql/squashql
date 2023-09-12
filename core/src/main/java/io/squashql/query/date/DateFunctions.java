package io.squashql.query.date;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateFunctions {

  public static final List<Pattern> DATE_PATTERNS = List.of(
          Pattern.compile("__(YEAR)__(.*)__"),
          Pattern.compile("__(QUARTER)__(.*)__"),
          Pattern.compile("__(MONTH)__(.*)__"));
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

  public static String extractFieldFromDateFunctionOrReturn(String str) {
    for (Pattern p : DATE_PATTERNS) {
      Matcher matcher = p.matcher(str);
      if (matcher.find()) {
        return matcher.group(2);
      }
    }
    return str;
  }

  public static boolean isDateFunction(String str) {
    for (Pattern p : DATE_PATTERNS) {
      Matcher matcher = p.matcher(str);
      if (matcher.find()) {
        return true;
      }
    }
    return false;
  }
}
