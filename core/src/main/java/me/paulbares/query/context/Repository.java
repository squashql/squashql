package me.paulbares.query.context;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class Repository implements ContextValue {

  public static final String KEY = "repository";

  public String url;

  public Repository(String url) {
    this.url = url;
  }

  @Override
  public String key() {
    return KEY;
  }
}
