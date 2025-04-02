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
    private List<String> allowedValues;
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
        	dataset = dataset.withColumn(columnName, functions.trim(functions.col(columnName)));
        	
        	Dataset<Row> cleaned = dataset
        		    .filter(
        		        functions.col(columnName).isNotNull()
        		            .and(functions.not(functions.lower(functions.col(columnName)).equalTo("null")))
        		    );
        	
            allowedValues = cleaned
                .groupBy(columnName)
                .count()
                .filter(functions.col("count").geq(MIN_OCCURRENCES))
                .select(columnName)
                .collectAsList()
                .stream()
                .map(row -> row.getString(0)  == null ? null : row.get(0).toString())
                .collect(Collectors.toList());
            
            List<Row> failingRows = cleaned
            	    .filter(functions.not(functions.col(columnName).isin(allowedValues.toArray())))
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
