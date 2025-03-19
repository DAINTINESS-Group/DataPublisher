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
        	String delimiter = dataset.sparkSession().conf().get("spark.sql.csv.delimiter", ";");
        	
        	org.apache.spark.sql.Column[] columns = Arrays.stream(dataset.columns())
                    .map(functions::col)
                    .toArray(org.apache.spark.sql.Column[]::new);
            long nonDataRowCount = dataset.filter(
                functions.lower(functions.concat_ws(delimiter, columns))
                .rlike(".*(date|modified|notes|comments|metadata|explanation).*")
            ).count();

            return nonDataRowCount == 0;
        } catch (Exception e) {
            System.err.println("Error executing CSV No Additional Info Check: " + e.getMessage());
            return false;
        }
    }

}
