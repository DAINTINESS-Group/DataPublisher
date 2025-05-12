package fairchecks.checks.columnChecks;

import fairchecks.api.IGenericColumnCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;

import java.util.ArrayList;
import java.util.List;

/**
 * A check that verifies whether null or missing values in a column are explicitly marked,
 * supporting data clarity and findability.
 *
 * <p>It considers a value invalid if it is either:
 * <ul>
 *   <li>SQL null</li>
 *   <li>An empty string ("")</li>
 * </ul>
 * <p>Check ID: FEU2
 */
public class NullValueMarkingCheck implements IGenericColumnCheck {
	
	private final String columnName;
    private final List<String> invalidRows = new ArrayList<>();

    public NullValueMarkingCheck(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String getCheckId() {
        return "FEU2";
    }

    @Override
    public String getCheckDescription() {
        return "Data with a null value should be marked as such.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
        List<Row> failingRows = dataset
                .filter(dataset.col(columnName).isNull()
                		.or(dataset.col(columnName).equalTo("")))
                .select(functions.col("_id"), functions.col(columnName))
                .collectAsList();

        for (Row row : failingRows) {
        	Number rowIdNum = (Number) row.getAs("_id");
        	long rowId = rowIdNum.longValue() + 1;
        	Object raw = row.get(1);
        	
        	String value = (raw == null) ? "<null>" : raw.toString();
        	invalidRows.add("Row " + rowId + ": Invalid null value found in: " + value);
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
