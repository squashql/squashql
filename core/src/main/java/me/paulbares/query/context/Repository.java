package me.paulbares.query.context;

import java.util.Objects;

public class Repository implements ContextValue {

  public static final String KEY = "repository";

  public String url;

  /**
   * Jackson.
   */
  public Repository() {
  }

  public Repository(String url) {
    this.url = url;
  }

  @Override
  public String key() {
    return KEY;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Repository totals = (Repository) o;
    return Objects.equals(this.url, totals.url);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.url);
  }

  @Override
  public String toString() {
    return "Repository{" +
            "url='" + url + '\'' +
            '}';
  }
}
