package fairchecks.checks.globalChecks;

import fairchecks.api.IGenericCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A global check that ensures datasets do not contain duplicate rows.
 * It identifies groups of rows that appear more than once and flags each duplicated instance.
 * 
 * <p>Check ID: REU3
 */
public class DuplicateDataCheck implements IGenericCheck {
	
	private final List<String> invalidRows = new ArrayList<>();

    @Override
    public String getCheckId() {
        return "REU3";
    }

    @Override
    public String getCheckDescription() {
        return "Data should not be duplicated.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
    	List<String> columnsToUse = Arrays.stream(dataset.columns())
                .filter(col -> !col.equalsIgnoreCase("_id"))
                .collect(Collectors.toList());

        for (String col : columnsToUse) {
            dataset = dataset.withColumn(col, functions.trim(functions.col(col).cast("string")));
        }

        Dataset<Row> duplicates = dataset
                .groupBy(columnsToUse.stream().map(functions::col).toArray(Column[]::new))
                .count()
                .filter("count > 1")
                .drop("count");

        List<Row> duplicateRows = duplicates.collectAsList();
        for (Row row : duplicateRows) {
            invalidRows.add("Duplicate row found: " + row.mkString(", "));
        }

        return invalidRows.isEmpty();
    }

    /*@Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }*/
}
