package io.squashql.util;

import java.math.BigDecimal;

public final class Types {

  private Types() {
  }

  public static double castToDouble(BigDecimal decimal) {
    double v = decimal.doubleValue();
    if (v == Double.MAX_VALUE || v == Double.MIN_VALUE) {
      // possible loss of info. abort
      throw new RuntimeException("Cannot convert " + decimal + " to double.");
    }
    return v;
  }
}
