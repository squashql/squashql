package io.squashql.query.dto;

import io.squashql.query.BasicMeasure;
import io.squashql.query.Field;
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
public class CriteriaDto implements Cloneable {

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

  @Override
  public CriteriaDto clone() {
    return new CriteriaDto(
            this.field,
            this.fieldOther,
            this.measure,
            this.condition,
            this.conditionType,
            this.children == null ? null : this.children.stream().map(CriteriaDto::clone).toList()
    );
  }

}
