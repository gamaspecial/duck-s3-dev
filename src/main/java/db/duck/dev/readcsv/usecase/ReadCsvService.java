package db.duck.dev.readcsv.usecase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.jooq.DSLContext;
import org.springframework.stereotype.Service;

import db.duck.dev.readcsv.domain.Header;
import lombok.AllArgsConstructor;

@Service
@AllArgsConstructor
public class ReadCsvService {

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
      try {
        var rs = stmt.executeQuery(String.format("""
              SELECT
                *
              FROM
              read_csv(
                '%s',
                columns = {'name': 'VARCHAR', 'age': 'INTEGER'},
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

          while (rs.next()) {
            for (int i = 1; i <= columnCount; i++) {
              String columnName = meta.getColumnName(i);
              String value = rs.getString(i);
              System.out.printf("%s = %s%n", columnName, value);
            }
            System.out.println("-----");
          }
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
      }
      return null;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

//    try {
////      dsl.execute(String.format("""
////          CREATE TABLE ontime AS SELECT * FROM read_csv_auto('%s');
////          """, s3Url)
////      );
////      var o1 = dsl.select().from("ontime").fetch();
////
////      // 生の SQL で取得するしかないか
//      var pragmaResult = dsl.fetch(String.format("DESCRIBE SELECT * FROM read_csv_auto('%s')", s3Url));
//
//      List<String> columnNames = pragmaResult.getValues("column_name", String.class);
//
//      var fieldsMap = IntStream.range(0, columnNames.size())
//          .boxed()
//          .collect(Collectors.toMap(
//              i -> i,
//              i -> DSL.field('"' + columnNames.get(i) + '"')
//          ));
//      var fields = List.of(
//          DSL.field(fieldsMap.get(1).getName(), Long.class).as("no"),
//          DSL.field(fieldsMap.get(2).getName(), String.class).as("customer_code"),
//          DSL.field(fieldsMap.get(0).getName(), LocalDate.class).as("date"),
//          DSL.field(fieldsMap.get(4).getName(), BigDecimal.class).as("amount")
//      );
//
//      var res = dsl.select(fields)
//          .from(String.format("""
//                    read_csv(
//                      '%s',
//                      store_rejects = true,
//                      rejects_scan = 'reject_scans',
//                      rejects_table = 'reject_errors'
//                    )
//                  """, s3Url
////            , header, skip
//          ))
//          .fetch();
//
//      return new Data(
//          fields.stream().map(f -> new Header(f.getName(), f.getType().getName())).toList(),
//          res.stream().map(record -> {
//            String[] row = new String[fields.size()];
//            for (int i = 0; i < fields.size(); i++) {
//              var value = record.get(fields.get(i));
//              row[i] = value != null ? value.toString() : null;
//            }
//            return row;
//          }).toList()
//      );
//    } catch (Exception e) {
////      var rejectScans = dsl.select().from("reject_scans").fetch();
////      var rejectErrors = dsl.select().from("reject_errors").fetch();
//
//      throw e;
//    }
  }
}
