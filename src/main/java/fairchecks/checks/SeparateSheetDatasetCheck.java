package fairchecks.checks;

import fairchecks.api.IReusabilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SeparateSheetDatasetCheck implements IReusabilityCheck {
	
	@Override
    public String getCheckId() {
        return "REU7.5";
    }

    @Override
    public String getCheckDescription() {
        return "For tabular data, each sheet should be published as a new dataset.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
        try {
            String sheetCountStr = dataset.sparkSession().conf().get("spark.sql.excel.sheetCount", "1");
            int sheetCount = Integer.parseInt(sheetCountStr);

            return sheetCount == 1;
        } catch (Exception e) {
            System.err.println("Error executing Separate Sheet Dataset Check: " + e.getMessage());
            return false;
        }
    }

}
