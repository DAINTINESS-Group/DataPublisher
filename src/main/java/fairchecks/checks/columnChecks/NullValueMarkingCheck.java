package fairchecks.checks.columnChecks;

import fairchecks.api.IGenericColumnCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
                .select(columnName)
                .collectAsList();

        for (Row row : failingRows) {
        	if (!row.isNullAt(0)) {
                invalidRows.add("Missing or improperly marked null value in: " + row.getString(0));
            } else {
                invalidRows.add("Missing or improperly marked null value: <null>");
            }
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
