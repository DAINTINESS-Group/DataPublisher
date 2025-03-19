package fairchecks.checks.columnChecks;

import fairchecks.api.IFindabilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.ArrayList;
import java.util.List;

public class NullValueMarkingCheck implements IFindabilityCheck {
	
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
            invalidRows.add("Missing or improperly marked null value at index " + row.getLong(0) + ": " + row.get(0));
        }

        return invalidRows.isEmpty();
    }

    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }

}
