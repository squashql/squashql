package me.paulbares.query.comp;

public class Comparisons {

  public static final String COMPARISON_METHOD_ABS_DIFF = "absolute_difference";
  public static final String COMPARISON_METHOD_REL_DIFF = "relative_difference";

  public static Object compare(String method, Object currentValue, Object referenceValue, Class<?> dataType) {
    return switch (method) {
      case COMPARISON_METHOD_ABS_DIFF -> computeAbsoluteDiff(currentValue, referenceValue, dataType);
      case COMPARISON_METHOD_REL_DIFF -> computeRelativeDiff(currentValue, referenceValue, dataType);
      default -> throw new IllegalArgumentException(String.format("Not supported comparison %s", method));
    };
  }

  public static Class<?> getOutputType(String method, Class<?> dataType) {
    return switch (method) {
      case COMPARISON_METHOD_ABS_DIFF -> dataType;
      case COMPARISON_METHOD_REL_DIFF -> double.class;
      default -> throw new IllegalArgumentException(String.format("Not supported comparison %s", method));
    };
  }

  private static double computeRelativeDiff(Object current, Object previous, Class<?> dataType) {
    if (dataType.equals(Double.class) || dataType.equals(double.class)) {
      return (((double) current) - ((double) previous)) / ((double) previous);
    } else if (dataType.equals(Float.class) || dataType.equals(float.class)) {
      return (((float) current) - ((float) previous)) / ((float) previous);
    } else if (dataType.equals(Integer.class) || dataType.equals(int.class)) {
      return (double) (((int) current) - ((int) previous)) / ((long) previous);
    } else if (dataType.equals(Long.class) || dataType.equals(long.class)) {
      return (double) (((long) current) - ((long) previous)) / ((long) previous);
    } else {
      throw new RuntimeException("Unsupported type " + dataType);
    }
  }

  private static Object computeAbsoluteDiff(Object current, Object previous, Class<?> dataType) {
    if (dataType.equals(Double.class) || dataType.equals(double.class)) {
      return ((double) current) - ((double) previous);
    } else if (dataType.equals(Float.class) || dataType.equals(float.class)) {
      return ((float) current) - ((float) previous);
    } else if (dataType.equals(Integer.class) || dataType.equals(int.class)) {
      return ((int) current) - ((int) previous);
    } else if (dataType.equals(Long.class) || dataType.equals(long.class)) {
      return ((long) current) - ((long) previous);
    } else {
      throw new RuntimeException("Unsupported type " + dataType);
    }
  }
}
