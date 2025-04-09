package db.duck.dev.readcsv.domain;

import lombok.Getter;

@Getter
public enum ImportField {
  CUST_CD(ImportFieldType.VARCHAR),
  AMOUNT(ImportFieldType.BIGINT),
  DATE(ImportFieldType.DATE);

  private final ImportFieldType type;

  ImportField(ImportFieldType type) {
    this.type = type;
  }
}
