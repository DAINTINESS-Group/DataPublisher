package fairchecks.checks.columnChecks;

import fairchecks.api.IInteroperabilityCheck;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class DateTimeFormatCheck implements IInteroperabilityCheck {
	
	private final String columnName;
	private final List<String> invalidRows = new ArrayList<>();

    public DateTimeFormatCheck(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String getCheckId() {
        return "IEU1";
    }

    @Override
    public String getCheckDescription() {
        return "The data must have a specific date and time format. The format must be encoded as ISO 8601 (YYYY-MM-DD hh:mm:ss). The time zone must be used and derived from Coordinated Universal Time (UTC).";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
    	String iso8601Regex = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}( [+-]\\d{2}:\\d{2})?$";

        List<Row> failingRows = dataset
                .filter(functions.col(columnName).isNotNull()
                        .and(functions.not(functions.col(columnName).rlike(iso8601Regex))))
                .select(columnName)
                .collectAsList();

        for (Row row : failingRows) {
        	Object rawVal = row.get(0);
            if (rawVal == null) continue;

            String value = rawVal.toString().trim();

            if (value.equalsIgnoreCase("null")) continue;
            
            invalidRows.add("Invalid date or time format: " + value);
        }

        return invalidRows.isEmpty();
    }
    
    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }

}
