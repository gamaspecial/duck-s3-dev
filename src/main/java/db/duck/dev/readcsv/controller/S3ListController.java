package db.duck.dev.readcsv.controller;

import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import db.duck.dev.readcsv.usecase.S3ListService;
import lombok.AllArgsConstructor;

@RestController
@AllArgsConstructor
@RequestMapping("/s3")
public class S3ListController {

  private final S3ListService s3ListService;

  // http://localhost:8080/s3/list?prefix=dev
  @GetMapping("/list")
  public List<String> listFiles(
      @RequestParam String prefix
  ) {
    String bucket = "duck-db-dev";
    return s3ListService.listFiles(bucket, prefix);
  }
}
