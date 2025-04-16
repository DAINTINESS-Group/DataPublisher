package fairchecks.checks.globalChecks;

import fairchecks.api.IInteroperabilityCheck;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class CsvSingleHeaderCheck implements IInteroperabilityCheck{
	
	@Override
    public String getCheckId() {
        return "IEU11.3";
    }

    @Override
    public String getCheckDescription() {
        return "CSV - Files should contain only one header line.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
    	try {
            String fileUri = dataset.inputFiles()[0];
            File file = new File(new URI(fileUri));

            Set<String> headerSet = new HashSet<>();
            boolean isFirstLine = true;

            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty()) {
                        continue;
                    }

                    if (isFirstLine) {
                        headerSet.add(line.trim());
                        isFirstLine = false;
                    } else {
                        if (headerSet.contains(line.trim())) {
                            return false;
                        }
                    }
                }
            }
            return true;
        } catch (Exception e) {
            System.err.println("Error executing CSV Single Header Check: " + e.getMessage());
            return false;
        }
    }

}
