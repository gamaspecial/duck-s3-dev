package db.duck.dev.readcsv.usecase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.jooq.DSLContext;
import org.springframework.stereotype.Service;

import db.duck.dev.readcsv.domain.Header;
import lombok.AllArgsConstructor;

@Service
@AllArgsConstructor
public class ReadCsvStmtService {

  private final DSLContext dsl;

  public record Data(
      List<Header> headers,
      List<String[]> rows
  ) {

  }

  public Data read(String s3Url, boolean header, int skip) {
    try {
      Connection conn = DriverManager.getConnection("jdbc:duckdb:duckdb.db");
      Statement stmt = conn.createStatement();
      Data data = null;
      try {
        var rs = stmt.executeQuery(String.format("""
              SELECT
                *
              FROM
              read_csv_auto(
                '%s',
                columns = {
                  'column00': 'INTEGER',
                  'column01': 'DATE',
                  'column02': 'VARCHAR'
                },
                store_rejects = true,
                rejects_scan = 'reject_scans',
                rejects_table = 'reject_errors',
                rejects_limit = 1000
              )
            """, s3Url
        ));
        try (rs) {
          ResultSetMetaData meta = rs.getMetaData();
          int columnCount = meta.getColumnCount();
          List<Header> headers = IntStream.range(1, columnCount + 1)
              .mapToObj(i -> {
                try {
                  return new Header(
                      meta.getColumnName(i),
                      meta.getColumnTypeName(i)
                  );
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              })
              .toList();

          List<String[]> rows = new ArrayList<>();
          while (rs.next()) {
            String[] row = new String[columnCount];
            for (int i = 1; i <= columnCount; i++) {
              row[i - 1] = rs.getString(i);
            }
            rows.add(row);
          }

          data = new Data(headers, rows);
        }

        var rejectErrors = conn.createStatement().executeQuery("SELECT * FROM reject_errors");
        try (rejectErrors) {
          ResultSetMetaData meta = rejectErrors.getMetaData();
          int columnCount = meta.getColumnCount();

          while (rejectErrors.next()) {
            for (int i = 1; i <= columnCount; i++) {
              String columnName = meta.getColumnName(i);
              String value = rejectErrors.getString(i);
              System.out.printf("%s = %s%n", columnName, value);
            }
            System.out.println("-----");
          }
        }

        var rejectScans = conn.createStatement().executeQuery("SELECT * FROM reject_scans");
        try (rejectScans) {
          ResultSetMetaData meta = rejectScans.getMetaData();
          int columnCount = meta.getColumnCount();

          while (rejectScans.next()) {
            for (int i = 1; i <= columnCount; i++) {
              String columnName = meta.getColumnName(i);
              String value = rejectScans.getString(i);
              System.out.printf("%s = %s%n", columnName, value);
            }
            System.out.println("-----");
          }
        }

        stmt.close();
        conn.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
      return data;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
