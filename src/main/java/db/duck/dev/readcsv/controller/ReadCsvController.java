package db.duck.dev.readcsv.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import db.duck.dev.readcsv.usecase.ReadCsvService;
import lombok.AllArgsConstructor;

@RestController
@AllArgsConstructor
@RequestMapping("/s3")
public class ReadCsvController {

  private final ReadCsvService readCsvService;

  private final static String bucketName = "duck-db-dev";

  // http://localhost:8080/s3/duck-read?key=sample_88ba926e-8bbb-4c16-8fe5-fd42c759e84f.csv&header=true&skip=0
  @GetMapping("/duck-read")
  public ReadCsvService.Data processCsv(
      @RequestParam String key,
      @RequestParam(defaultValue = "true") boolean header,
      @RequestParam(defaultValue = "0") int skip
  ) {
    String s3Url = "s3://" + bucketName + "/" + key;
    return readCsvService.read(s3Url, header, skip);
  }
}
