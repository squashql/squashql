package io.squashql.query.dto;

import io.squashql.query.Field;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class PivotTableQueryDto {

  public QueryDto query;
  public List<Field> rows; //todo-mde should we use alias since we know rows and columns are already in the query ?
  public List<Field> columns;
}
