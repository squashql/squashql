package io.squashql.query.dto;

import io.squashql.query.ColumnSet;
import io.squashql.query.ColumnSetKey;
import io.squashql.type.TableTypedField;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
  public List<TableTypedField> getNewColumns() {
    return List.of(new TableTypedField(null, this.name, String.class), new TableTypedField(null, this.field, String.class));
  }

  @Override
  public ColumnSetKey getColumnSetKey() {
    return ColumnSetKey.BUCKET;
  }
}
