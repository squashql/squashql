package io.squashql.store;

import com.fasterxml.jackson.annotation.JsonIgnore;

public record FieldWithStore(String store, String name, Class<?> type) {

  @JsonIgnore
  public String getFullName() {
    return buildFullName(this.store, this.name);
  }

  public static String buildFullName(String store, String name) {
    return store == null ? name : store + "." + name;
  }
}
