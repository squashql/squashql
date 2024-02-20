package io.squashql.jackson;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.util.List;

public class SquashQLTypeFactory {

  public static JavaType listOf(Class<?> clazz) {
    return TypeFactory.defaultInstance().constructCollectionType(List.class, clazz);
  }

  public static JavaType of(Class<?> clazz) {
    return TypeFactory.defaultInstance().constructType(clazz);
  }
}
