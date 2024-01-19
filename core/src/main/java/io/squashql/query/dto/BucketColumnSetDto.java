package io.squashql.query.dto;

import io.squashql.query.ColumnSet;
import io.squashql.query.ColumnSetKey;
import io.squashql.query.NamedField;
import io.squashql.query.TableField;
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

  public NamedField newField;

  public NamedField field;

  public Map<String, List<String>> values = new LinkedHashMap<>();

  public BucketColumnSetDto(String name, NamedField field) {
    this.newField = new TableField(name);
    this.field = field;
  }

  public BucketColumnSetDto withNewBucket(String bucketName, List<String> bucketValues) {
    this.values.put(bucketName, new ArrayList<>(bucketValues));
    return this;
  }

  @Override
  public List<NamedField> getColumnsForPrefetching() {
    return List.of(this.field);
  }

  @Override
  public List<NamedField> getNewColumns() {
    return List.of(this.newField, this.field);
  }

  @Override
  public ColumnSetKey getColumnSetKey() {
    return ColumnSetKey.BUCKET;
  }
}
