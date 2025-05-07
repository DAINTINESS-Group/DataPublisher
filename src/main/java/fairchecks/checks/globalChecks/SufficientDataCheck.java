package fairchecks.checks.globalChecks;

import fairchecks.api.IGenericCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A global check that verifies whether a dataset contains a sufficient amount of data
 * to be considered meaningful and useful for reuse.
 *
 * <p>Check ID: REU1
 */
public class SufficientDataCheck implements IGenericCheck {
	
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
