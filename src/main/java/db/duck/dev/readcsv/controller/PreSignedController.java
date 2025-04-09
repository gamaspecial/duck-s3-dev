package db.duck.dev.readcsv.controller;

import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lombok.AllArgsConstructor;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

@RestController
@AllArgsConstructor
@RequestMapping("/s3")
public class PreSignedController {

  private static final long MAX_FILE_SIZE = 10 * 1024 * 1024; // 10MB

  // http://localhost:8080/s3/presigned?filename=sample&prefix=dev
  @GetMapping("/presigned")
  public String getPreSignedUrl(@RequestParam String filename, @RequestParam String prefix) {
    String bucket = "duck-db-dev";
    String key = prefix + "/" + filename + ".csv";

    try (S3Presigner presigner = S3Presigner.builder()
        .region(Region.AP_NORTHEAST_1)
        .credentialsProvider(DefaultCredentialsProvider.create())
        .build()) {

      PutObjectRequest putObjectRequest = PutObjectRequest.builder()
          .bucket(bucket)
          .key(key)
          .contentType("text/csv")
          .overrideConfiguration(
              o -> o.putHeader("If-None-Match", List.of("*"))
          )
          .build();

      PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
          .signatureDuration(Duration.ofMinutes(10)) // 有効期限10分
          .putObjectRequest(putObjectRequest)
          .build();

      PresignedPutObjectRequest presignedRequest = presigner.presignPutObject(presignRequest);

      URL url = presignedRequest.url();

      return url.toString();
    }
  }
}
