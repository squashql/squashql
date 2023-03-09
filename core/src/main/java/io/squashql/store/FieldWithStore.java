package io.squashql.store;

public record FieldWithStore(String store, String name, Class<?> type) {

  public String getFullName() {
    return this.store == null ? this.name : this.store + "." + this.name;
  }
}
