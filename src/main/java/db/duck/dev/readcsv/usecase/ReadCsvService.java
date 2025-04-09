package db.duck.dev.readcsv.usecase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.jooq.impl.DSL;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import db.duck.dev.readcsv.domain.CsvImportFormat;
import db.duck.dev.readcsv.domain.Header;
import db.duck.dev.readcsv.domain.ImportField;
import db.duck.dev.readcsv.domain.ImportFieldType;
import db.duck.dev.readcsv.domain.ImportMethod;
import lombok.AllArgsConstructor;

@Service
@AllArgsConstructor
public class ReadCsvService {

  //  private final DSLContext dsl;
  private final DataSource dataSource;

  public record Data(
      List<Header> headers,
      List<String[]> rows
  ) {

  }

  @Transactional
  public Data read(String s3Url, boolean header, int skip) {

    // format
    var format = new CsvImportFormat(
        1, "",
        Map.of(
            ImportField.CUST_CD, new ImportMethod.FromCsvColumn(2),
            ImportField.AMOUNT, new ImportMethod.FromCsvColumn(0),
            ImportField.DATE, new ImportMethod.FromCsvColumn(1)
        )
    );

    var con = DataSourceUtils.getConnection(dataSource);
    var dsl = DSL.using(con);

    // 1. 対象S3 CSVのカラム一覧を取得
    var pragmaResult = dsl.fetch(String.format("DESCRIBE SELECT * FROM read_csv_auto('%s')", s3Url));
    List<String> columnNames = pragmaResult.getValues("column_name", String.class);

    // <order, カラム> formatで設定されているカラムを取得
    var selectedColumns = format.getMappings().entrySet().stream()
        .filter(entry -> entry.getValue() instanceof ImportMethod.FromCsvColumn)
        .collect(Collectors.toMap(
            entry -> ((ImportMethod.FromCsvColumn) entry.getValue()).columnIndex(),
            Map.Entry::getKey
        ));

    // <カラム名, 型名> CSVヘッダ一覧
    var columns = IntStream.range(0, columnNames.size())
        .boxed()
        .collect(Collectors.toMap(
            columnNames::get,
            i -> Optional.ofNullable(selectedColumns.get(i)).map(ImportField::getType).orElse(ImportFieldType.VARCHAR),
            (v1, v2) -> v1,
            LinkedHashMap::new
        ));

    // read_csv_auto のカラム設定
    String columnsScan = columns.entrySet().stream()
        .map(entry -> String.format("'%s': '%s'", entry.getKey(), entry.getValue()))
        .collect(Collectors.joining(", "));

    var res = dsl.fetch(
        String.format(
            """
                  SELECT
                    %s
                  FROM
                  read_csv_auto(
                    '%s',
                    columns = {{ %s }},
                    store_rejects = true,
                    rejects_scan = 'reject_scans',
                    rejects_table = 'reject_errors',
                    rejects_limit = 1000
                  )
                """,
            s3Url,
            columnsScan
        )
    );

    // エラー情報の取得
    var rejectErrors = dsl.select()
        .from("reject_errors")
        .fetch();
    printResult(rejectErrors);
    var rejectScans = dsl.select()
        .from("reject_scans")
        .fetch();
    printResult(rejectScans);

    return new Data(
        columnNames.stream().map(f -> new Header(f, f)).toList(),
        res.stream().map(record -> {
          String[] row = new String[columnNames.size()];
          for (int i = 0; i < columnNames.size(); i++) {
            var value = record.get(columnNames.get(i));
            row[i] = value != null ? value.toString() : null;
          }
          return row;
        }).toList()
    );
  }

  private void printResult(org.jooq.Result<?> result) {
    for (var record : result) {
      for (var field : result.fields()) {
        var columnName = field.getName();
        var value = record.get(field);
        System.out.printf("%s = %s%n", columnName, value);
      }
      System.out.println("-----");
    }
  }
}
