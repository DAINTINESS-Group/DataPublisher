package fairchecks.checks;

import fairchecks.api.IInteroperabilityCheck;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Arrays;

public class CsvNoAdditionalInfoCheck implements IInteroperabilityCheck{
	
	@Override
    public String getCheckId() {
        return "IEU11.1";
    }

    @Override
    public String getCheckDescription() {
        return "CSV - Data should not contain explanations, dates, modifications, sheet names, etc.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
        try {        	
        	String delimiter = dataset.sparkSession().conf().get("spark.sql.csv.delimiter", ",");

            // Concatenate all columns into a single string
            org.apache.spark.sql.Column concatenatedColumns = functions.concat_ws(delimiter, Arrays.stream(dataset.columns())
                    .map(functions::col)
                    .toArray(org.apache.spark.sql.Column[]::new));

            // Define a regular expression pattern to detect non-data rows
            String pattern = "(?i)^(#.*|.*(last\\s*modified|sheet\\s*name|notes|comments|metadata|explanation).*)$";

            // Filter rows matching the pattern
            long nonDataRowCount = dataset.filter(concatenatedColumns.rlike(pattern)).count();

            // If any non-data rows are found, the check fails
            return nonDataRowCount == 0;
        } catch (Exception e) {
            System.err.println("Error executing CSV No Additional Info Check: " + e.getMessage());
            return false;
        }
    }

}
