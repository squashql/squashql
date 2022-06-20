package me.paulbares.query.dto;

import me.paulbares.query.ColumnSet;
import me.paulbares.store.Field;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.ArrayList;
import java.util.List;

public class BucketColumnSetDto implements ColumnSet {

  public String name;

  public String field;

  public List<Pair<String, List<String>>> values = new ArrayList<>();

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
    this.values.add(Tuples.pair(bucketName, bucketValues));
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
}
