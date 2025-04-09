package db.duck.dev.readcsv.domain;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class CsvImportFormat {

  private long id;
  private String name;

  private Map<ImportField, ImportMethod> mappings;
//  private List<ImportField> duplicateCheckKeyFields;
}
