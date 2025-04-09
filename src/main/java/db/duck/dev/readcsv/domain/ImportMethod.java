package db.duck.dev.readcsv.domain;

public sealed interface ImportMethod permits
    ImportMethod.FromCsvColumn,
    ImportMethod.FixedValue,
    ImportMethod.Ignored {
  // Marker interface for import methods
  // No additional methods or fields are needed

  record FromCsvColumn(int columnIndex) implements ImportMethod {

  }

  record FixedValue(String value) implements ImportMethod {

  }

  record Ignored() implements ImportMethod {

  }
}
