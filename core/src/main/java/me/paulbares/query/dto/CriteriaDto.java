package me.paulbares.query.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static me.paulbares.query.dto.ConditionType.AND;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class CriteriaDto {

  public static final CriteriaDto NO_CRITERIA = new CriteriaDto(AND, Collections.emptyList());

  public String field;
  public ConditionDto condition;
  public ConditionType conditionType;
  public List<CriteriaDto> children;

  public CriteriaDto(String field, ConditionDto condition) {
    this.field = field;
    this.condition = condition;
  }

  public CriteriaDto(ConditionType conditionType, List<CriteriaDto> criteriaDtos) {
    this.conditionType = conditionType;
    this.children = criteriaDtos;
  }

  public static CriteriaDto deepCopy(CriteriaDto criteriaDto) {
    if (criteriaDto.field != null) {
      return new CriteriaDto(criteriaDto.field, criteriaDto.condition);
    } else {
      List<CriteriaDto> list = new ArrayList<>(criteriaDto.children.size());
      for (CriteriaDto dto : criteriaDto.children) {
        CriteriaDto copy = deepCopy(dto);
        list.add(copy);
      }
      return new CriteriaDto(criteriaDto.conditionType, list);
    }
  }

  @JsonIgnore
  public boolean isCriterion() {
    return this.field != null;
  }
}
