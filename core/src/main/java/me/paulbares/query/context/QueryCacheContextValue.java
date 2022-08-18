package me.paulbares.query.context;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class QueryCacheContextValue implements ContextValue {

  public static final String KEY = "cache";

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
}
