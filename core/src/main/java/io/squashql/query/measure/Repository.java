package io.squashql.query.measure;

import com.fasterxml.jackson.databind.JavaType;
import io.squashql.jackson.SquashQLTypeFactory;
import io.squashql.query.field.Field;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class Repository {

  public static final String VAR = "VAR";
  public static final String INCREMENTAL_VAR = "INCREMENTAL_VAR";

  private Repository() {
  }

  public static Measure create(ParametrizedMeasure m) {
    String methodName = switch (m.key) {
      case VAR -> "var";
      case INCREMENTAL_VAR -> "incrementalVar";
      default -> throw new IllegalArgumentException("unknown " + ParametrizedMeasure.class + ": " + m);
    };

    try {
      Method var = Arrays.stream(ParametrizedMeasureFactory.class.getDeclaredMethods()).filter(method -> method.getName().equals(methodName)).findFirst().get();
      List<String> parameterNames = getParameterTypes(m.key).stream().map(Pair::getOne).toList();
      List<Object> args = new ArrayList<>();
      args.add(m.alias);
      for (String paramName : parameterNames) {
        args.add(m.parameters.get(paramName));
      }
      return (Measure) var.invoke(null, args.toArray(new Object[0]));
    } catch (IllegalAccessException |
             InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<Pair<String, JavaType>> getParameterTypes(String key) {
    JavaType fieldJT = SquashQLTypeFactory.of(Field.class);
    JavaType doubleJT = SquashQLTypeFactory.of(double.class);
    JavaType listOfFieldsJT = SquashQLTypeFactory.listOf(Field.class);
    if (key.equals(VAR)) {
      return List.of(Tuples.pair("value", fieldJT), Tuples.pair("date", fieldJT), Tuples.pair("quantile", doubleJT));
    } else if (key.equals(INCREMENTAL_VAR)) {
      return List.of(Tuples.pair("value", fieldJT), Tuples.pair("date", fieldJT), Tuples.pair("quantile", doubleJT), Tuples.pair("ancestors", listOfFieldsJT));
    } else {
      throw new IllegalArgumentException("unknown key: " + key);
    }
  }
}
