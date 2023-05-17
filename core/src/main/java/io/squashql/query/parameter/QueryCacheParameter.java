package io.squashql.query.context;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class QueryCacheParameter implements Parameter {

  public static final String KEY = "cache";

  public QueryCacheParameter(Action action) {
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
