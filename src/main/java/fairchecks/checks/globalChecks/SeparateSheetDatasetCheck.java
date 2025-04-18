package fairchecks.checks.globalChecks;

import fairchecks.api.IReusabilityCheck;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A global check that ensures each sheet in an Excel file is published as a separate dataset,
 * rather than bundling multiple tables into one file.
 *
 * <p>Check ID: REU7.5
 */
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
    		String path = dataset.inputFiles()[0];
            
            if (!path.toLowerCase().endsWith(".xlsx") && !path.toLowerCase().endsWith(".xls")) {
            	return true;
            }
            
            File excelFile = new File(new java.net.URI(path));
            try (FileInputStream fis = new FileInputStream(excelFile);
                 Workbook workbook = WorkbookFactory.create(fis)) {

                int numberOfSheets = workbook.getNumberOfSheets();
                return numberOfSheets == 1;
            }

        } catch (IOException e) {
            System.err.println("Error reading Excel file: " + e.getMessage());
            return false;
        } catch (Exception e) {
            System.err.println("Error in SingleExcelSheetCheck: " + e.getMessage());
            return false;
        }
    }
}
