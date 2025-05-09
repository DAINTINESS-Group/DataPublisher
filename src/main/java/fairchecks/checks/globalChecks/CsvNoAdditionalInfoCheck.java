package fairchecks.checks.globalChecks;

import fairchecks.api.IGenericCheck;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Arrays;

/**
 * A global check that ensures CSV data files do not contain extraneous
 * non-tabular information such as notes, metadata, explanations, or sheet names.
 * 
 * <p>Check ID: IEU11.1
 */
public class CsvNoAdditionalInfoCheck implements IGenericCheck{
	
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

            org.apache.spark.sql.Column concatenatedColumns = functions.concat_ws(delimiter, Arrays.stream(dataset.columns())
                    .map(functions::col)
                    .toArray(org.apache.spark.sql.Column[]::new));

            String pattern = "(?i)^(#.*|.*(last\\s*modified|sheet\\s*name|notes|comments|metadata|explanation).*)$";

            long nonDataRowCount = dataset.filter(concatenatedColumns.rlike(pattern)).count();

            return nonDataRowCount == 0;
        } catch (Exception e) {
            System.err.println("Error executing CSV No Additional Info Check: " + e.getMessage());
            return false;
        }
    }
}
