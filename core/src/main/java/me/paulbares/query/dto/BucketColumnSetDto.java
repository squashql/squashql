package me.paulbares.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import me.paulbares.query.ColumnSet;
import me.paulbares.query.ColumnSetKey;
import me.paulbares.store.TypedField;

import java.util.*;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class BucketColumnSetDto implements ColumnSet {

  public String name;

  public String field;

  public Map<String, List<String>> values = new LinkedHashMap<>();

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
  public List<TypedField> getNewColumns() {
    return List.of(new TypedField(this.name, String.class), new TypedField(this.field, String.class));
  }

  @Override
  public ColumnSetKey getColumnSetKey() {
    return ColumnSetKey.BUCKET;
  }
}
