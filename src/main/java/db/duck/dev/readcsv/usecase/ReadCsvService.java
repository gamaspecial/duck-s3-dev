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

import db.duck.dev.readcsv.domain.Header;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

@Service
public class ReadCsvService {

  public record Data(
      List<Header> headers,
      List<String[]> rows
  ) {

  }

  public Data read(String s3Url, boolean header, int skip) {
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

      ResultSet rs = stmt.executeQuery(String.format("""
          SELECT * FROM
            read_csv(
              '%s',
              header=%s,
              skip=%s
            )
          """, s3Url, header, skip));

      ResultSetMetaData meta = rs.getMetaData();
      int columnCount = meta.getColumnCount();

      var headers = new ArrayList<Header>();
      for (int i = 1; i <= columnCount; i++) {
        headers.add(new Header(meta.getColumnName(i), meta.getColumnTypeName(i)));
      }

      List<String[]> results = new ArrayList<>();

      while (rs.next()) {
        String[] row = new String[columnCount];
        for (int i = 1; i <= columnCount; i++) {
          row[i - 1] = rs.getString(i);
        }
        results.add(row);
      }

      return new Data(headers, results);
    } catch (SQLException e) {
      throw new RuntimeException("DuckDB S3直接読み込みエラー", e);
    }
  }
}

//    sep VARCHAR
//    delim VARCHAR
//    parallel BOOLEAN
//    nullstr ANY
//    quote VARCHAR
//    normalize_names BOOLEAN
//    new_line VARCHAR
//    encoding VARCHARq
//    escape VARCHAR
//    maximum_line_size VARCHAR
//    columns ANY
//    strict_mode BOOLEAN
//    column_names VARCHAR[]
//    auto_type_candidates ANY
//    header BOOLEAN
//    auto_detect BOOLEAN
//    sample_size BIGINT
//    all_varchar BOOLEAN
//    dateformat VARCHAR
//    timestampformat VARCHAR
//    compression VARCHAR
//    filename ANY
//    comment VARCHAR
//    skip BIGINT
//    max_line_size VARCHAR
//    ignore_errors BOOLEAN
//    types ANY
//    store_rejects BOOLEAN
//    rejects_table VARCHAR
//    rejects_scan VARCHAR
//    union_by_name BOOLEAN
//    rejects_limit BIGINT
//    force_not_null VARCHAR[]
//    buffer_size UBIGINT
//    decimal_separator VARCHAR
//    null_padding BOOLEAN
//    allow_quoted_nulls BOOLEAN
//    column_types ANY
//    dtypes ANY
//    names VARCHAR[]
//    hive_partitioning BOOLEAN
//    hive_types ANY
//    hive_types_autocast BOOLEAN
