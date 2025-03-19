package fairchecks.checks.columnChecks;

import fairchecks.api.IInteroperabilityCheck;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class ThousandsSeparatorCheck implements IInteroperabilityCheck  {
	
	private final String columnName;
	private final List<String> invalidRows = new ArrayList<>();

    public ThousandsSeparatorCheck(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String getCheckId() {
        return "IEU2.2";
    }

    @Override
    public String getCheckDescription() {
        return "Thousands must not use separators.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
    	String thousandsSeparatorRegex = ".*[ ,][0-9]{3}.*";

        List<Row> failingRows = dataset
                .filter(functions.col(columnName).isNotNull()
                        .and(functions.col(columnName).rlike(thousandsSeparatorRegex)))
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
