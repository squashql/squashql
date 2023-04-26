package io.squashql.query.exception;

public class FieldNotFoundException extends RuntimeException {

  public FieldNotFoundException(String message) {
    super(message);
  }
}
