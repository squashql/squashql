package me.paulbares.query.context;

import java.util.Objects;

public class QueryCacheContextValue implements ContextValue {

  public static final String KEY = "cache";

  /**
   * Jackson.
   */
  public QueryCacheContextValue() {
  }

  public QueryCacheContextValue(Action action) {
    this.action = action;
  }

  public enum Action {
    USE, NOT_USE, INVALIDATE
  }

  public Action action;

  @Override
  public String key() {
    return KEY;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    QueryCacheContextValue queryCacheContextValue = (QueryCacheContextValue) o;
    return this.action == queryCacheContextValue.action;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.action);
  }
}
