package io.squashql.query.dto;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class JoinDto implements Cloneable {

  public TableDto table;
  public JoinType type; // inner|left
  public CriteriaDto joinCriteria;

  @Override
  public JoinDto clone() {
    return new JoinDto(
            this.table.clone(),
            this.type,
            this.joinCriteria == null ? null : this.joinCriteria.clone()
    );
  }
}
