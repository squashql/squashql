package io.squashql.query.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.squashql.query.BasicMeasure;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.squashql.query.dto.ConditionType.AND;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
public class CriteriaDto {

  public static final CriteriaDto NO_CRITERIA = new CriteriaDto(AND, Collections.emptyList());

  public String field;
  public String fieldOther;
  public BasicMeasure measure;
  public ConditionDto condition;
  public ConditionType conditionType;
  public List<CriteriaDto> children;

  public CriteriaDto(String field, ConditionDto condition) {
    this.field = field;
    this.condition = condition;
  }

  public CriteriaDto(BasicMeasure measure, ConditionDto condition) {
    this.measure = measure;
    this.condition = condition;
  }

  public CriteriaDto(ConditionType conditionType, List<CriteriaDto> criteriaDtos) {
    this.conditionType = conditionType;
    this.children = criteriaDtos;
  }

  public CriteriaDto(String field, String fieldOther, ConditionType conditionType) {
    this.field = field;
    this.fieldOther = fieldOther;
    this.conditionType = conditionType;
  }

  public static CriteriaDto deepCopy(CriteriaDto criteriaDto) {
    if (criteriaDto.isWhereCriterion()) {
      return new CriteriaDto(criteriaDto.field, criteriaDto.condition);
    } else if (criteriaDto.isHavingCriterion()) {
      return new CriteriaDto(criteriaDto.measure, criteriaDto.condition);
    } else if (criteriaDto.isJoinCriterion()) {
      return new CriteriaDto(criteriaDto.field, criteriaDto.fieldOther, criteriaDto.conditionType);
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
  public boolean isWhereCriterion() {
    return this.field != null && this.condition != null;
  }

  @JsonIgnore
  public boolean isHavingCriterion() {
    return this.measure != null && this.condition != null;
  }

  @JsonIgnore
  public boolean isJoinCriterion() {
    return this.field != null && this.fieldOther != null && this.conditionType != null;
  }

  @JsonIgnore
  public boolean isCriteria() {
    return this.conditionType != null && this.children != null && !this.children.isEmpty();
  }
}
