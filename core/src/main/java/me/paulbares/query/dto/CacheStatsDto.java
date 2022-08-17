package me.paulbares.query.dto;

import lombok.*;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@Getter
@Builder
public class CacheStatsDto {
  public long hitCount;
  public long missCount;
  public long evictionCount;

  public CacheStatsDto(long hitCount, long missCount, long evictionCount) {
    this.hitCount = hitCount;
    this.missCount = missCount;
    this.evictionCount = evictionCount;
  }
}
