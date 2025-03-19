package fairchecks.checks.columnChecks;

import fairchecks.api.IInteroperabilityCheck;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class SpecialCharacterCheck implements IInteroperabilityCheck {
	
	private final String columnName;
	private final List<String> invalidRows = new ArrayList<>();

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
    	String specialCharRegex = ".*[!@#$%^&*()_+={}\\[\\]:;\"'<>,.?/\\\\].*";

        List<Row> failingRows = dataset
                .filter(functions.col(columnName).isNotNull()
                        .and(functions.col(columnName).rlike(specialCharRegex)))
                .select(columnName)
                .collectAsList();

        for (Row row : failingRows) {
            invalidRows.add(row.getString(0));
        }

        return invalidRows.isEmpty();
    }
    
    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }

}
