package fairchecks.checks.globalChecks;

import fairchecks.api.IInteroperabilityCheck;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CsvSemicolonSeparatorCheck implements IInteroperabilityCheck{
	
	@Override
    public String getCheckId() {
        return "IEU9";
    }

    @Override
    public String getCheckDescription() {
        return "CSV - Data shall use the semicolon, not the comma, as a separator between values, with no spaces or tabs on either side.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
        try {
            String delimiter = dataset.sparkSession().conf().get("spark.sql.csv.delimiter", ",");
            System.out.println("the delimiter is: "+ delimiter);
            return delimiter.equals(";");
        } catch (Exception e) {
            System.err.println("Error executing CSV Semicolon Separator Check: " + e.getMessage());
            return false;
        }
    }

}
