package io.squashql.spring.web.rest;

import io.squashql.query.exception.LimitExceedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.ServletRequest;

@ControllerAdvice
@Order(1)
public class SquashQLErrorHandler {

  /**
   * The logger.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(SquashQLErrorHandler.class.getName());

  @ExceptionHandler(value = LimitExceedException.class)
  @ResponseBody
  public ResponseEntity<String> limit(ServletRequest req, LimitExceedException ex) {
    LOGGER.error("", ex);
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("query limit exceeded");
  }
}
