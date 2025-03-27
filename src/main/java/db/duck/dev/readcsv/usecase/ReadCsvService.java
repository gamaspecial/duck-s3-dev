package db.duck.dev.readcsv.usecase;

import java.util.List;
import java.util.stream.IntStream;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;
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
    var pragmaResult = dsl.fetch(String.format("DESCRIBE SELECT * FROM read_csv_auto('%s')", s3Url));

    List<String> columnNames = pragmaResult.getValues("column_name", String.class);
    List<String> columnTypes = pragmaResult.getValues("column_type", String.class);

    List<Header> headers = IntStream.range(0, columnNames.size())
        .mapToObj(i -> new Header(columnNames.get(i), columnTypes.get(i)))
        .toList();

    var fields = columnNames.stream()
        .map(name -> DSL.field('"' + name + '"'))  // ← 明示的にダブルクォートで囲む
        .toList();

    var res = dsl.select(fields)
        .from(String.format("""
              read_csv(
                '%s'
              )
            """, s3Url, header, skip))
        .fetch();

    var rows = res.stream().map(record -> {
      String[] row = new String[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        row[i] = record.get(i, String.class);
      }
      return row;
    }).toList();

    return new Data(headers, rows);
  }
}
