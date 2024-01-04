package io.squashql.query.exception;

public class LimitExceedException extends RuntimeException {

  public LimitExceedException(String message) {
    super(message);
  }
}
