package me.paulbares.dto;

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
}
