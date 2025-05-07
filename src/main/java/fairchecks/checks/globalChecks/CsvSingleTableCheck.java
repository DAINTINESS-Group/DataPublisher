package fairchecks.checks.globalChecks;

import fairchecks.api.IGenericCheck;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;

/**
 * A global check that verifies whether a CSV file contains exactly one table.
 * 
 * <p>Check ID: IEU10
 */
public class CsvSingleTableCheck implements IGenericCheck{
	
	@Override
    public String getCheckId() {
        return "IEU10";
    }

    @Override
    public String getCheckDescription() {
        return "CSV - Each data file shall contain a single table.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
    	try {
            String encodedPath = dataset.inputFiles()[0];
            
            File csvFile;
            if (encodedPath.startsWith("file:/")) {
                URI uri = URI.create(encodedPath);
                csvFile = new File(uri);
            } else {
                csvFile = new File(encodedPath);
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(csvFile), StandardCharsets.UTF_8))) {
				int firstHeaderIndex = -1;
				int secondHeaderIndex = -1;

				int lineIndex = 0;
				String line;
				while ((line = reader.readLine()) != null) {
				    String trimmed = line.trim();
				    if (trimmed.isEmpty()) {
				        lineIndex++;
				        continue;
				    }

				    String[] columns = trimmed.split(",", -1);
				    long nonEmptyCount = java.util.Arrays.stream(columns).filter(cell -> !cell.trim().isEmpty()).count();

				    if (nonEmptyCount > 3) {
				        int offset = countLeadingEmptyCells(columns);

				        if (firstHeaderIndex == -1) {
				            firstHeaderIndex = lineIndex;
				        } else if (secondHeaderIndex == -1) {
				            int blankLinesBetween = lineIndex - firstHeaderIndex - 1;

				            if (blankLinesBetween >= 2 && offset >= 2) {
				                secondHeaderIndex = lineIndex;
				                break;
				            }
				        }
				    }

				    lineIndex++;
				}

				boolean hasSecondTable = (secondHeaderIndex != -1);
				return !hasSecondTable;
			}
            
        } catch (Exception e) {
            System.err.println("Error executing Single Table Check: " + e.getMessage());
            return false;
        }
    }
    
    private int countLeadingEmptyCells(String[] cells) {
        int count = 0;
        for (String cell : cells) {
            if (cell.trim().isEmpty()) count++;
            else break;
        }
        return count;
    }
}
