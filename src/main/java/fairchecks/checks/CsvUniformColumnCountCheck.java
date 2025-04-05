package fairchecks.checks;

import fairchecks.api.IInteroperabilityCheck;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.net.URI;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

//import com.sleepycat.je.log.FileReader;

public class CsvUniformColumnCountCheck implements IInteroperabilityCheck {
	
	@Override
    public String getCheckId() {
        return "IEU16";
    }

    @Override
    public String getCheckDescription() {
        return "CSV - Data files should have the same number of columns for all rows.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
    	try {
            String fileUri = dataset.inputFiles()[0];
            File file = new File(new URI(fileUri));
            
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String header = reader.readLine();
                if (header == null) return false;

                int expectedColumns = header.split(",").length;
                int lineNumber = 1;
                String line;
                while ((line = reader.readLine()) != null) {
                    lineNumber++;
                    if (line.trim().isEmpty()) continue;

                    int actualColumns = line.split(",", -1).length;

                    if (actualColumns != expectedColumns) {
                        return false;
                    }
                }
                return true;
            }
        } catch (Exception e) {
            System.err.println("Error executing CSV Uniform Column Count Check: " + e.getMessage());
            return false;
        }
    }

}
