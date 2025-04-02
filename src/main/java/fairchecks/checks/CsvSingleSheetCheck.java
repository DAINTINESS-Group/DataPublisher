package fairchecks.checks;

import fairchecks.api.IInteroperabilityCheck;
import utils.DatasetType;
import utils.DatasetTypeDetector;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvSingleSheetCheck implements IInteroperabilityCheck{
	
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
        	SparkSession spark = dataset.sparkSession();
            DatasetType datasetType = DatasetTypeDetector.detectDatasetType(spark);

            if (datasetType != DatasetType.CSV) {
                System.out.println("Skipping CSV check because dataset is not CSV.");
                return true;
            }
        	
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
