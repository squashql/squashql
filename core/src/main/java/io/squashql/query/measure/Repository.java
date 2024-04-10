package io.squashql.query.measure;

import com.fasterxml.jackson.databind.JavaType;
import io.squashql.jackson.SquashQLTypeFactory;
import io.squashql.query.Axis;
import io.squashql.query.Field;
import io.squashql.query.Measure;
import org.eclipse.collections.api.tuple.Triple;
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

  public static Measure create(ParametrizedMeasure pm) {
    String methodName = switch (pm.key) {
      case VAR -> "var";
      case INCREMENTAL_VAR -> "incrementalVar";
      default -> throw new IllegalArgumentException("unknown " + ParametrizedMeasure.class + ": " + pm);
    };

    try {
      Method method = Arrays.stream(ParametrizedMeasureFactory.class.getDeclaredMethods()).filter(m -> m.getName().equals(methodName)).findFirst().get();
      List<String> parameterNames = getParameterTypes(pm.key).stream().map(Triple::getOne).toList();
      List<Boolean> isMandatory = getParameterTypes(pm.key).stream().map(Triple::getThree).toList();
      List<Object> args = new ArrayList<>();
      args.add(pm.alias);
      for (int i = 0; i < parameterNames.size(); i++) {
        Object e = pm.parameters.get(parameterNames.get(i));
        if (e == null) {
          if (isMandatory.get(i)) {
            throw new IllegalArgumentException(String.format("Parameter '%s' was expected but not provided in %s", parameterNames.get(i), pm));
          }
          continue;
        }
        args.add(e);
      }
      return (Measure) method.invoke(null, args.toArray(new Object[0]));
    } catch (IllegalAccessException |
             InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Boolean -> is mandatory?
   */
  public static List<Triple<String, JavaType, Boolean>> getParameterTypes(String key) {
    JavaType fieldJT = SquashQLTypeFactory.of(Field.class);
    JavaType doubleJT = SquashQLTypeFactory.of(double.class);
    JavaType listOfFieldsJT = SquashQLTypeFactory.listOf(Field.class);
    JavaType axis = SquashQLTypeFactory.of(Axis.class);
    JavaType str = SquashQLTypeFactory.of(String.class);
    if (key.equals(VAR)) {
      return List.of(Tuples.triple("value", fieldJT, true), Tuples.triple("date", fieldJT, true), Tuples.triple("quantile", doubleJT, true), Tuples.triple("return", str, true));
    } else if (key.equals(INCREMENTAL_VAR)) {
      return List.of(Tuples.triple("value", fieldJT, true), Tuples.triple("date", fieldJT, true), Tuples.triple("quantile", doubleJT, true), Tuples.triple("ancestors", listOfFieldsJT, true), Tuples.triple("axis", axis, false));
    } else {
      throw new IllegalArgumentException("unknown key: " + key);
    }
  }
}
