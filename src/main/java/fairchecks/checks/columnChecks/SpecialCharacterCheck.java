package fairchecks.checks.columnChecks;

import fairchecks.api.IGenericColumnCheck;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;

/**
 * A check that verifies whether values in a column contain special characters.
 * <p>It flags any non-null value that includes characters outside letters, digits,
 * and whitespace and excludes ",", ":", "-", "+", ".", using the regex {@code .*[^\\p{Alnum}\\s,\\.\\-\\+:''].*}.
 *
 * <p>Check ID: IEU3.2
 */
public class SpecialCharacterCheck implements IGenericColumnCheck {
	
	private final String columnName;
	private final List<String> invalidRows = new ArrayList<>();
	
	private static final String specialCharRegex = ".*[^\\p{Alnum}\\s,\\.\\-\\+:''].*";

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
                .select(functions.col("_id"), functions.col(columnName))
                .collectAsList();

        for (Row row : rowsWithSpecialChars) {
        	Number rowIdNum = (Number) row.getAs("_id");
        	long rowId = rowIdNum.longValue() + 1;
            Object raw = row.get(1);
            
            String value = (raw == null) ? "<null>" : raw.toString();
            invalidRows.add("Row " + rowId + ": Special character found in value: "+ value);
        }

        return invalidRows.isEmpty();
    }
    
    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }
    
    @Override
    public boolean isApplicable(DataType columnType) {
        return true;
    }
}
