package me.paulbares.query;

import me.paulbares.store.Field;

import java.util.function.Function;

/**
 * Marker interface.
 */
public interface Measure {

  String sqlExpression(Function<String, Field> fieldProvider);

  String alias();
}
