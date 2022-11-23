package me.paulbares.query.dto;

import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class CriterionDto {

  public String field;
  public ConditionDto conditionDto;
  public List<CriterionDto> criterionDtos;
  public ConditionType conditionType;

  public CriterionDto(String field, ConditionDto conditionDto) {
    this.field = field;
    this.conditionDto = conditionDto;
  }

  public CriterionDto(ConditionType conditionType, List<CriterionDto> criterionDtos) {
    this.conditionType = conditionType;
    this.criterionDtos = criterionDtos;
  }
}
