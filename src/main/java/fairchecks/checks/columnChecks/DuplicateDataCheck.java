package fairchecks.checks.columnChecks;

import fairchecks.api.IReusabilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

//TODO maybe put in global??
public class DuplicateDataCheck implements IReusabilityCheck {
	
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
    	Column[] groupByColumns = Stream.of(dataset.columns()).map(functions::col).toArray(Column[]::new);

        if (groupByColumns.length == 0) {
            return true;
        }

        Dataset<Row> duplicates = dataset
                .groupBy(groupByColumns)
                .count()
                .filter("count > 1")
                .drop("count");

        List<Row> failingRows = duplicates.collectAsList();

        for (Row row : failingRows) {
            invalidRows.add("Duplicate row found at index " + row.getLong(0) + ": " + row.mkString(", "));
        }

        return invalidRows.isEmpty();
    }

    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }
}
