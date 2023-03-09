package io.squashql.store;

import com.fasterxml.jackson.annotation.JsonIgnore;

public record FieldWithStore(String store, String name, Class<?> type) {

  @JsonIgnore
  public String getFullName() {
    return this.store == null ? this.name : this.store + "." + this.name;
  }
}
