package me.paulbares.query;

import me.paulbares.store.Field;

import java.util.function.Function;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Marker interface.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface Measure {

  String sqlExpression(Function<String, Field> fieldProvider);

  String alias();
}
