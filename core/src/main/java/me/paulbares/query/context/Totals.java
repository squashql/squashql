package me.paulbares.query.context;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class Totals implements ContextValue {

  public static final String KEY = "totals";
  public static final String POSITION_TOP = "top";
  public static final String POSITION_BOTTOM = "bottom";

  public String position;

  public Totals(String position) {
    this.position = position;
  }

  @Override
  public String key() {
    return KEY;
  }
}
