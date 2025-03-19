package fairchecks.checks.columnChecks;

import fairchecks.api.IReusabilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DataAccuracyCheck implements IReusabilityCheck {
	
	private final String columnName;
    private List<String> allowedValues; // Inferred dynamically from common values
    private final List<String> invalidRows = new ArrayList<>();
    private static final int MIN_OCCURRENCES = 3;

    public DataAccuracyCheck(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String getCheckId() {
        return "REU4";
    }

    @Override
    public String getCheckDescription() {
        return "Data should be accurate by conforming to commonly expected values.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
        try {
            allowedValues = dataset
                .groupBy(columnName)
                .count()
                .filter(functions.col("count").geq(MIN_OCCURRENCES))
                .select(columnName)
                .collectAsList()
                .stream()
                .map(row -> row.getString(0))
                .collect(Collectors.toList());

            // Step 2: Find inaccurate rows (values not in the frequent term list)
            List<Row> failingRows = dataset
                    .filter(dataset.col(columnName).isNotNull()
                            .and(functions.not(dataset.col(columnName).isin(allowedValues.toArray()))))
                    .select(columnName)
                    .collectAsList();

            for (Row row : failingRows) {
                invalidRows.add("Uncommon or inaccurate value: " + row.getString(0));
            }

            return invalidRows.isEmpty();
        } catch (Exception e) {
            System.err.println("Error executing Data Accuracy Check: " + e.getMessage());
            return false;
        }
    }

    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }

    public List<String> getInferredAllowedValues() {
        return allowedValues;
    }

}
