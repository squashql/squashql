package io.squashql.query.dto;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class TableDto implements Cloneable {

  public String name;
  public QueryDto subQuery;
  public List<JoinDto> joins = new ArrayList<>();
  public boolean isCte = false;

  public TableDto(String name) {
    if (this.subQuery != null) {
      throw new IllegalStateException();
    }
    this.name = name;
  }

  public TableDto(QueryDto subQuery) {
    if (this.name != null) {
      throw new IllegalStateException();
    }
    this.subQuery = subQuery;
  }

  public void join(TableDto other, JoinType joinType, CriteriaDto joinCriteria) {
    this.joins.add(new JoinDto(other, joinType, joinCriteria));
  }

  @Override
  public TableDto clone() {
    return new TableDto(
            this.name,
            this.subQuery == null ? null : this.subQuery.clone(),
            new ArrayList<>(this.joins),
            this.isCte
    );
  }
}
