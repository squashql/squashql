package me.paulbares.query.dto;

import me.paulbares.query.ColumnSet;
import me.paulbares.store.Field;

import java.util.*;

public class BucketColumnSetDto implements ColumnSet {

  public String name;

  public String field;

  public Map<String, List<String>> values = new LinkedHashMap<>();

  /**
   * For Jackson.
   */
  public BucketColumnSetDto() {
  }

  public BucketColumnSetDto(String name, String field) {
    this.name = name;
    this.field = field;
  }

  public BucketColumnSetDto withNewBucket(String bucketName, List<String> bucketValues) {
    this.values.put(bucketName, new ArrayList<>(bucketValues));
    return this;
  }

  @Override
  public List<String> getColumnsForPrefetching() {
    return List.of(this.field);
  }

  @Override
  public List<Field> getNewColumns() {
    return List.of(new Field(this.name, String.class), new Field(this.field, String.class));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BucketColumnSetDto that = (BucketColumnSetDto) o;
    return Objects.equals(this.name, that.name) && Objects.equals(this.field, that.field) && Objects.equals(this.values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.name, this.field, this.values);
  }
}