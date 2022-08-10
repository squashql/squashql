package me.paulbares.jackson.deserializer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import me.paulbares.query.dto.ExplicitOrderDto;
import me.paulbares.query.dto.OrderDto;
import me.paulbares.query.dto.OrderKeywordDto;
import me.paulbares.query.dto.SimpleOrderDto;

import java.io.IOException;
import java.util.List;

public class OrderDtoDeserializer extends JsonDeserializer<OrderDto> {
  
  @Override
  public OrderDto deserialize(JsonParser p, DeserializationContext deserializationContext) throws IOException {
    JsonNode treeNode = p.getCodec().readTree(p);
    if (treeNode.has("explicit")) {
      List explicit = p.getCodec().treeToValue(treeNode.get("explicit"), List.class);
      return new ExplicitOrderDto(explicit);
    } else if (treeNode.has("order")) {
      OrderKeywordDto order = OrderKeywordDto.valueOf(treeNode.get("order").asText());
      return new SimpleOrderDto(order);
    } else {
      throw new IllegalArgumentException("current: " + p.currentName() + "; " + treeNode);
    }
  }
}
