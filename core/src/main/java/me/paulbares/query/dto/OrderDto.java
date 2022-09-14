package me.paulbares.query.dto;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Marker interface.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public sealed interface OrderDto permits ExplicitOrderDto, SimpleOrderDto {
}
