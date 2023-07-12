package io.squashql.query.dto;

import lombok.*;

@Builder
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class DebugInfoDto {

  public CacheStatsDto cache;
}
