package me.paulbares.spring.web.rest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloController {

  public static final String MESSAGE = "Hello world!";

  @GetMapping("/")
  public String index() {
    return MESSAGE;
  }
}