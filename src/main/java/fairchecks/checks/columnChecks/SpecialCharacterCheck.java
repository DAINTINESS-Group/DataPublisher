package fairchecks.checks.columnChecks;

import fairchecks.api.IInteroperabilityCheck;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * A check that verifies whether values in a column contain special characters.
 * <p>It flags any non-null value that includes characters outside letters, digits,
 * and whitespace using the regex {@code .*[^\\p{Alnum}\\s].*}.
 *
 * <p>Check ID: IEU3.2
 */
public class SpecialCharacterCheck implements IInteroperabilityCheck {
	
	private final String columnName;
	private final List<String> invalidRows = new ArrayList<>();
	
	private static final String specialCharRegex = ".*[^\\p{Alnum}\\s].*";

    public SpecialCharacterCheck(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String getCheckId() {
        return "IEU3.2";
    }

    @Override
    public String getCheckDescription() {
        return "Data shall not consist of special characters.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
    	List<Row> rowsWithSpecialChars = dataset
                .filter(functions.col(columnName).rlike(specialCharRegex))
                .select(columnName)
                .collectAsList();

        for (Row row : rowsWithSpecialChars) {
            if (!row.isNullAt(0)) {
                invalidRows.add("Special character found in value: " + row.get(0).toString());
            } else {
                invalidRows.add("Special character found in value: <null>");
            }
        }

        return invalidRows.isEmpty();
    }
    
    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }
}
