package fairchecks.checks.columnChecks;

import fairchecks.api.IGenericApplicableCheck;
import fairchecks.api.IGenericCheckWithInvalidRows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * A check that validates whether date/time values in a column conform to the ISO 8601 format.
 *
 * <p>The expected format is: {@code YYYY-MM-DD hh:mm:ss ±hh:mm}, where:
 * <ul>
 *   <li>Date and time must be present</li>
 *   <li>Time zone (optional) must be derived from UTC (e.g., +00:00)</li>
 * </ul>
 *
 * <p>Only string-typed columns that contain date/time-related keywords in their names are applicable.
 *
 * <p>Check ID: IEU1
 */
public class DateTimeFormatCheck implements IGenericCheckWithInvalidRows, IGenericApplicableCheck {
	
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
    
    @Override
    public boolean isApplicable(DataType columnType) {
    	List<String> keywords = Arrays.asList("date", "time", "datetime", "timestamp");

        if (!columnType.equals(DataTypes.StringType)) return false;

        String col = columnName.toLowerCase();
        return keywords.stream().anyMatch(col::contains);
    }
}
