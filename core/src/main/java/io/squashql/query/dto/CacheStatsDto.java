package io.squashql.query.dto;

import lombok.*;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@Getter
public class CacheStatsDto {
  public long hitCount;
  public long missCount;
  public long evictionCount;

  public CacheStatsDto(long hitCount, long missCount, long evictionCount) {
    this.hitCount = hitCount;
    this.missCount = missCount;
    this.evictionCount = evictionCount;
  }

  public static CacheStatsDtoBuilder builder() {
    return new CacheStatsDtoBuilder();
  }

  public static class CacheStatsDtoBuilder {
    private long hitCount;
    private long missCount;
    private long evictionCount;

    CacheStatsDtoBuilder() {
    }

    public CacheStatsDtoBuilder hitCount(long hitCount) {
      this.hitCount = hitCount;
      return this;
    }

    public CacheStatsDtoBuilder missCount(long missCount) {
      this.missCount = missCount;
      return this;
    }

    public CacheStatsDtoBuilder evictionCount(long evictionCount) {
      this.evictionCount = evictionCount;
      return this;
    }

    public CacheStatsDto build() {
      return new CacheStatsDto(this.hitCount, this.missCount, this.evictionCount);
    }
  }
}
