package fairchecks.checks.globalChecks;

import fairchecks.api.IInteroperabilityCheck;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A global check that ensures each file (specifically Excel files) contains only one sheet.
 *
 * <p>Check ID: IEU11.4
 */
public class SingleSheetCheck implements IInteroperabilityCheck{
	
	@Override
    public String getCheckId() {
        return "IEU11.4";
    }

    @Override
    public String getCheckDescription() {
        return "CSV - Each file should contain only one sheet.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
        try {        	
            String filePath = dataset.sparkSession().conf().get("spark.sql.csv.filepath", "").toLowerCase();
            if (!filePath.endsWith(".xlsx") && !filePath.endsWith(".xls")) {
                return true;
            }

            String sheetCountStr = dataset.sparkSession().conf().get("spark.sql.excel.sheetCount", "1");
            long sheetCount = Long.parseLong(sheetCountStr);
            return sheetCount == 1;
        } catch (Exception e) {
            System.err.println("Error executing Single Sheet Check: " + e.getMessage());
            return false;
        }
    }
}
