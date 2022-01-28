package me.paulbares.query.dto;

import java.util.Objects;

public class JoinMappingDto {

  public String from;
  public String to;

  /**
   * For Jackson.
   */
  public JoinMappingDto() {
  }

  public JoinMappingDto(String from, String to) {
    this.from = from;
    this.to = to;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    JoinMappingDto that = (JoinMappingDto) o;
    return Objects.equals(this.from, that.from) && Objects.equals(this.to, that.to);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.from, this.to);
  }

  @Override
  public String toString() {
    return "JoinMappingDto{" +
            "from='" + from + '\'' +
            ", to='" + to + '\'' +
            '}';
  }
}
