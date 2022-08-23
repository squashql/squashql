package me.paulbares.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import me.paulbares.query.Measure;

import java.util.List;

@NoArgsConstructor
@EqualsAndHashCode
@ToString
public class MetadataResultDto {

  public List<StoreMetadata> stores;
  public List<String> aggregation_functions;
  public List<Measure> measures;

  public MetadataResultDto(List<StoreMetadata> stores, List<String> aggregation_functions, List<Measure> measures) {
    this.stores = stores;
    this.aggregation_functions = aggregation_functions;
    this.measures = measures;
  }

  @NoArgsConstructor
  @EqualsAndHashCode
  @ToString
  public static class StoreMetadata {

    public String name;
    public List<MetadataItem> fields;

    public StoreMetadata(String name, List<MetadataItem> fields) {
      this.name = name;
      this.fields = fields;
    }
  }
}
