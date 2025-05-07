package fairchecks.checks.globalChecks;

import fairchecks.api.IGenericCheck;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Paths;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A global check that verifies whether a CSV file uses a semicolon (";") as the value separator,
 * instead of a comma (",") or other delimiters.
 * 
 * <p>Check ID: IEU9
 */
public class CsvSemicolonSeparatorCheck implements IGenericCheck{
	
	private static final int MAX_LINES_TO_SAMPLE = 10;
	
	@Override
    public String getCheckId() {
        return "IEU9";
    }

    @Override
    public String getCheckDescription() {
        return "CSV - Data shall use the semicolon, not the comma, as a separator between values, with no spaces or tabs on either side.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
        try {
            String uriPath = dataset.inputFiles()[0];
            URI uri = new URI(uriPath);
            String filePath = Paths.get(uri).toString();

            int linesChecked = 0;
            int semicolonOkCount = 0;

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)))) {
                String line;
                while ((line = reader.readLine()) != null && linesChecked < MAX_LINES_TO_SAMPLE) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    linesChecked++;

                    if (isSemicolonSeparated(line)) {
                        semicolonOkCount++;
                    }
                }
            }

            if (linesChecked == 0) {
                System.out.println("No lines to check (empty file?).");
                return false;
            }

            double validRatio = (double) semicolonOkCount / linesChecked;

            return validRatio >= 0.8;

        } catch (Exception e) {
            System.err.println("Error executing Semicolon Separator Check: " + e.getMessage());
            return false;
        }
    }

	private boolean isSemicolonSeparated(String line) {
	    int semicolonCount = 0;
	    int commaOutsideQuotesCount = 0;
	
	    boolean insideQuotes = false;
	    for (char c : line.toCharArray()) {
	        if (c == '"') {
	            insideQuotes = !insideQuotes;
	        } else if (c == ';' && !insideQuotes) {
	            semicolonCount++;
	        } else if (c == ',' && !insideQuotes) {
	            commaOutsideQuotesCount++;
	        }
	    }

	    return semicolonCount >= 1 && commaOutsideQuotesCount == 0;
	}
}
