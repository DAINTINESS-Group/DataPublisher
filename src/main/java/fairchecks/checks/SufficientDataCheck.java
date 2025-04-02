package fairchecks.checks;

import fairchecks.api.IReusabilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SufficientDataCheck implements IReusabilityCheck {
	
	@Override
    public String getCheckId() {
        return "REU1";
    }

    @Override
    public String getCheckDescription() {
        return "The data published should be sufficient and useful.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
        try {
            long rowCount = dataset.count();
            int columnCount = dataset.columns().length;

            return rowCount >= 10 && columnCount >= 3;
        } catch (Exception e) {
            System.err.println("Error executing Sufficient Data Check: " + e.getMessage());
            return false;
        }
    }

}
