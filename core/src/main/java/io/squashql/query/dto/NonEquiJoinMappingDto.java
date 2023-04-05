package io.squashql.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.EnumSet;

import static io.squashql.query.dto.ConditionType.*;

/**
 * Mapping to perform a <b>NON</b> equi-join. Internal use only for now.
 */
@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class NonEquiJoinMappingDto extends JoinMappingDto {

  private static final EnumSet<ConditionType> supportedTypes = EnumSet.of(LT, LE, GT, GE, NEQ);

  public ConditionType conditionType;

  public NonEquiJoinMappingDto(String fromTable, String from, String toTable, String to, ConditionType conditionType) {
    super(fromTable, from, toTable, to);
    if (!supportedTypes.contains(conditionType)) {
      throw new IllegalArgumentException("Unexpected type for non equi-join: " + conditionType);
    }
    this.conditionType = conditionType;
  }
}
