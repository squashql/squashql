package io.squashql.query;

public class TotalCountMeasure extends ExpressionMeasure {

  public static final TotalCountMeasure INSTANCE = new TotalCountMeasure();
  public static final String ALIAS = "_total_count_";
  public static final String EXPRESSION = "COUNT(*) OVER ()";

  private TotalCountMeasure() {
    super(ALIAS, EXPRESSION);
  }

}
