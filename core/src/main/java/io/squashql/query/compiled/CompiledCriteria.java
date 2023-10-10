package io.squashql.query.compiled;

import io.squashql.query.TableField;
import io.squashql.query.database.QueryRewriter;
import io.squashql.query.dto.ConditionDto;
import io.squashql.query.dto.ConditionType;
import io.squashql.type.TypedField;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.squashql.query.database.SQLTranslator.toSql;

public record CompiledCriteria(TypedField field, TypedField fieldOther, CompiledMeasure measure, ConditionDto condition, ConditionType conditionType, List<CompiledCriteria> children){

  public String sqlExpression(QueryRewriter queryRewriter) {
    if (this.field != null && this.condition != null) {
      return toSql(this.field, this.condition, queryRewriter);
    } else if (this.measure != null && this.condition != null) {
      return toSql(new TableField(this.measure.alias()), this.condition, queryRewriter);
    } else if (this.field != null && this.fieldOther != null && this.conditionType != null) {
      String left = this.field.sqlExpression(queryRewriter);
      String right = this.fieldOther.sqlExpression(queryRewriter);
      return String.join(" ", left, this.conditionType.sqlInfix, right);
    } else if (!this.children.isEmpty()) {
      String sep = switch (this.conditionType) {
        case AND -> " and ";
        case OR -> " or ";
        default -> throw new IllegalStateException("Unexpected value: " + this.conditionType);
      };
      Iterator<CompiledCriteria> iterator = this.children.iterator();
      List<String> conditions = new ArrayList<>();
      while (iterator.hasNext()) {
        String c = iterator.next().sqlExpression(queryRewriter);
        if (c != null) {
          conditions.add(c);
        }
      }
      return conditions.isEmpty() ? null : ("(" + String.join(sep, conditions) + ")");
    } else {
      return null;
    }
  }

}
