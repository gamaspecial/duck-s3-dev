package db.duck.dev.readcsv.usecase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Service;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

@Service
public class ReadCsvService {

  public List<String[]> read(String s3Url) {
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = conn.createStatement();
        DefaultCredentialsProvider credentials = DefaultCredentialsProvider.create()) {

      stmt.execute(String.format("""
              CREATE SECRET s3_cred(
                  TYPE s3,
                  KEY_ID '%s',
                  SECRET '%s',
                  REGION '%s'
              );
              """,
          credentials.resolveCredentials().accessKeyId(),
          credentials.resolveCredentials().secretAccessKey(),
          new DefaultAwsRegionProviderChain().getRegion().id()
      ));
      stmt.execute("INSTALL httpfs");
      stmt.execute("LOAD httpfs");

      ResultSet rs = stmt.executeQuery(String.format("SELECT * FROM '%s'", s3Url));

      ResultSetMetaData meta = rs.getMetaData();
      int columnCount = meta.getColumnCount();

      List<String[]> results = new ArrayList<>();

      while (rs.next()) {
        String[] row = new String[columnCount];
        for (int i = 1; i <= columnCount; i++) {
          row[i - 1] = rs.getString(i);
        }
        results.add(row);
      }

      return results;
    } catch (SQLException e) {
      throw new RuntimeException("DuckDB S3直接読み込みエラー", e);
    }
  }
}
