package me.paulbares.query.context;

import java.util.Objects;

public class Totals implements ContextValue {

  public static final String KEY = "totals";
  public static final String POSITION_TOP = "top";
  public static final String POSITION_BOTTOM = "bottom";

  public String position;

  /**
   * Jackson.
   */
  public Totals() {
  }

  public Totals(String position) {
    this.position = position;
  }

  @Override
  public String key() {
    return KEY;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Totals totals = (Totals) o;
    return Objects.equals(this.position, totals.position);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.position);
  }

  @Override
  public String toString() {
    return "Totals{" +
            "position='" + position + '\'' +
            '}';
  }
}
