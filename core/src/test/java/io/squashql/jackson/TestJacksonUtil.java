package io.squashql.jackson;

import io.squashql.query.TableField;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.measure.ParametrizedMeasure;
import io.squashql.query.measure.Repository;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Map;

public class TestJacksonUtil {

  @Test
  void testLocalDate() {
    LocalDate d = LocalDate.of(2000, 3, 15);
    String serialize = JacksonUtil.serialize(d);
    LocalDate deserialize = JacksonUtil.deserialize(serialize, LocalDate.class);
    Assertions.assertThat(deserialize).isEqualTo(d);
  }

  @Test
  void testTableField() {
    TableField d = new TableField("myTable.myField");
    QueryDto queryDto = new QueryDto();
    queryDto.table("myTable");
    queryDto.withColumn(d);
    ParametrizedMeasure m = new ParametrizedMeasure("var", Repository.VAR, Map.of("value", d));
    queryDto.withMeasure(m);
    String serialize = JacksonUtil.serialize(queryDto);
    QueryDto deserialize = JacksonUtil.deserialize(serialize, QueryDto.class);
    Assertions.assertThat(deserialize).isEqualTo(d);
  }
}
