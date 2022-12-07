package me.paulbares.spring;


import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Error handler that prints the exception in standard error stream and also send to stacktrace to the client. Mainly to
 * be used for dev only.
 */
@ControllerAdvice
public class GlobalErrorHandler {

  @ExceptionHandler(Exception.class)
  public ResponseEntity<String> handleException(Exception exception) {
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    exception.printStackTrace(printWriter);
    exception.printStackTrace();
    return ResponseEntity.internalServerError().body(stringWriter.toString());
  }
}
