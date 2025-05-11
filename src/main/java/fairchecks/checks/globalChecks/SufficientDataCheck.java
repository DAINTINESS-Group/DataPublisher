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
            if (rowCount < 10 || columnCount < 3) {
            	return false;
            }
            
            long totalCells = rowCount * columnCount;
            long nullCount = dataset.javaRDD().map(row -> {
            	int nulls = 0;
            	for (int i = 0; i < row.size(); i++) {
            		Object val = row.get(i);
            		if (val== null || (val instanceof String && ((String) val).trim().equalsIgnoreCase("null"))) {
            			nulls++;
            		}
            	}
            	return nulls;
            }).reduce(Integer::sum);
            
            double nullRatio = (double) nullCount / totalCells;
            if (nullRatio > 0.5) {
            	return false;
            }
            return true;
            
        } catch (Exception e) {
            System.err.println("Error executing Sufficient Data Check: " + e.getMessage());
            return false;
        }
    }
}
