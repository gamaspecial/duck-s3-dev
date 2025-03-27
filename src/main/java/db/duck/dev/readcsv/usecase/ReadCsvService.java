package db.duck.dev.readcsv.usecase;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.jooq.DSLContext;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.springframework.stereotype.Service;

import db.duck.dev.readcsv.domain.Header;
import lombok.AllArgsConstructor;

@Service
@AllArgsConstructor
public class ReadCsvService {

  private final DSLContext dsl;

  private static final Map<String, DataType<?>> TYPE_MAPPING = Map.ofEntries(
      Map.entry("INTEGER", SQLDataType.INTEGER),
      Map.entry("BIGINT", SQLDataType.BIGINT),
      Map.entry("DOUBLE", SQLDataType.DOUBLE),
      Map.entry("DOUBLE PRECISION", SQLDataType.DOUBLE),
      Map.entry("FLOAT", SQLDataType.REAL),
      Map.entry("BOOLEAN", SQLDataType.BOOLEAN),
      Map.entry("DATE", SQLDataType.DATE),
      Map.entry("TIMESTAMP", SQLDataType.TIMESTAMP),
      Map.entry("VARCHAR", SQLDataType.VARCHAR),
      Map.entry("TEXT", SQLDataType.VARCHAR)
  );

  private static DataType<?> mapToDataType(String type) {
    return TYPE_MAPPING.getOrDefault(type.toUpperCase(), SQLDataType.VARCHAR);
  }

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

    var fields = IntStream.range(0, columnNames.size())
        .mapToObj(i -> DSL.field('"' + columnNames.get(i) + '"', mapToDataType(columnTypes.get(i))))
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
        Object value = record.get(fields.get(i)); // Field<T> から型安全に取得
        row[i] = value != null ? value.toString() : null;
      }
      return row;
    }).toList();

    return new Data(headers, rows);
  }
}
