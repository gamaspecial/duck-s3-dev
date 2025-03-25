package db.duck.dev.readcsv.controller;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import db.duck.dev.readcsv.usecase.ReadCsvService;
import lombok.AllArgsConstructor;

@RestController
@AllArgsConstructor
@RequestMapping("/csv")
public class ReadCsvController {

  private final ReadCsvService readCsvService;

  private final static String bucketName = "duck-db-dev";

  @GetMapping("/read")
  public List<String[]> processCsv(@RequestParam String key) throws IOException, SQLException {
    String s3Url = "s3://" + bucketName + "/" + key;
    return readCsvService.read(s3Url);
  }
}
