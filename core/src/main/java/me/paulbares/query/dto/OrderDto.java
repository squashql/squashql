package me.paulbares.query.dto;

/**
 * Marker interface.
 */
public sealed interface OrderDto permits ExplicitOrderDto, SimpleOrderDto {
}
