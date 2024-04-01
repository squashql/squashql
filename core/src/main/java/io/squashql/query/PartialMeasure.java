package io.squashql.query;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Marker interface.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface PartialMeasure extends Measure {

}
