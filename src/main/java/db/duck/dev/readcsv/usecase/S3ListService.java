package db.duck.dev.readcsv.usecase;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Service;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

@Service
public class S3ListService {

  public List<String> listFiles(String bucket, String prefix) {
    try (S3Client s3 = S3Client.create()) {

      ListObjectsV2Request request = ListObjectsV2Request.builder()
          .bucket(bucket)
          .prefix(prefix)
          .build();

      ListObjectsV2Response response = s3.listObjectsV2(request);

      List<String> keys = new ArrayList<>();
      for (S3Object object : response.contents()) {
        keys.add(object.key());
      }

      return keys;
    }
  }
}
