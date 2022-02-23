package me.paulbares.spring;


import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.io.PrintWriter;
import java.io.StringWriter;

@ControllerAdvice
public class GlobalErrorHandler {

  @ExceptionHandler(Exception.class)
  public ResponseEntity<String> handleException(Exception exception) {
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    exception.printStackTrace(printWriter);
    return ResponseEntity.internalServerError().body(stringWriter.toString());
  }
}
