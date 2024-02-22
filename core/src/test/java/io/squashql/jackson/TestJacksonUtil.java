package io.squashql.jackson;

import io.squashql.query.Field;
import io.squashql.query.Measure;
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
    TableField tableField = new TableField("myTable.myField");
    ParametrizedMeasure pm = new ParametrizedMeasure("var", Repository.VAR, Map.of("value", tableField));

    QueryDto query = new QueryDto();
    query.table("myTable");
    query.withColumn(tableField);
    query.withMeasure(pm);

    String tableFieldSer = JacksonUtil.serialize(tableField);
    Field tableFieldDes = JacksonUtil.deserialize(tableFieldSer, Field.class);
    Assertions.assertThat(tableFieldDes).isEqualTo(tableField);

    String pmSer = JacksonUtil.serialize(pm);
    Measure pmDes = JacksonUtil.deserialize(pmSer, Measure.class);
    Assertions.assertThat(pmDes).isEqualTo(pm);

    String querySer = JacksonUtil.serialize(query);
    QueryDto queryDes = JacksonUtil.deserialize(querySer, QueryDto.class);
    Assertions.assertThat(queryDes).isEqualTo(query);
  }
}
