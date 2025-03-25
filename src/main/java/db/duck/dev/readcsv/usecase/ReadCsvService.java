package db.duck.dev.readcsv.usecase;

import java.util.Arrays;
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
    var res = dsl.selectFrom(String.format("""
              read_csv(
                '%s',
                header=%s,
                skip=%s
              )
            """, s3Url, header, skip))
        .fetch();

    // head get
    var headers = Arrays.stream(res.fields()).map(
        field -> new Header(field.getName(), field.getDataType().getTypeName())
    ).toList();

    // value get
    var rows = res.stream().map(record -> {
      String[] row = new String[res.fields().length];
      for (int i = 0; i < res.fields().length; i++) {
        row[i] = record.get(i, String.class);
      }
      return row;
    }).toList();

    return new Data(headers, rows);
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
