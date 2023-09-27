package io.squashql.query.dto;

import io.squashql.query.BasicMeasure;
import io.squashql.query.Field;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class CriteriaDto {

  public Field field;
  public Field fieldOther;
  public BasicMeasure measure;
  public ConditionDto condition;
  public ConditionType conditionType;
  public List<CriteriaDto> children = new ArrayList<>();

  public CriteriaDto(Field field, ConditionDto condition) {
    this.field = field;
    this.condition = condition;
  }

  public CriteriaDto(BasicMeasure measure, ConditionDto condition) {
    this.measure = measure;
    this.condition = condition;
  }

  public CriteriaDto(Field field, Field fieldOther, ConditionType conditionType) {
    this.field = field;
    this.fieldOther = fieldOther;
    this.conditionType = conditionType;
  }

  public CriteriaDto(ConditionType conditionType, List<CriteriaDto> criteriaDtos) {
    this.conditionType = conditionType;
    this.children = criteriaDtos;
  }

  public static CriteriaDto deepCopy(CriteriaDto criteriaDto) {
    if (criteriaDto.children == null || criteriaDto.children.isEmpty()) {
      return new CriteriaDto(
              criteriaDto.field,
              criteriaDto.fieldOther,
              criteriaDto.measure,
              criteriaDto.condition,
              criteriaDto.conditionType,
              Collections.emptyList());
    } else {
      List<CriteriaDto> list = new ArrayList<>(criteriaDto.children.size());
      for (CriteriaDto dto : criteriaDto.children) {
        CriteriaDto copy = deepCopy(dto);
        list.add(copy);
      }
      return new CriteriaDto(criteriaDto.conditionType, list);
    }
  }
}
