package fairchecks.checks.globalChecks;

import fairchecks.api.IInteroperabilityCheck;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * A global check that ensures the title of the dataset is expressed in the distribution's metadata
 * (i.e., the filename), and not redundantly repeated within the contents of the CSV file itself.
 *
 * <p>Check ID: IEU11.2
 */
public class CsvTitleInDistributionCheck implements IInteroperabilityCheck{
	
	@Override
    public String getCheckId() {
        return "IEU11.2";
    }

    @Override
    public String getCheckDescription() {
        return "CSV - The title should be represented within the distribution name, and not contained in the actual file.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
        try {
        	String fileUri = dataset.inputFiles()[0];
            File file = new File(new URI(fileUri));

            String fileName = file.getName().replaceFirst("[.][^.]+$", "").replace("_", " ").toLowerCase();

            boolean containsFileName = Arrays.stream(dataset.columns()).anyMatch(column ->
            	dataset.filter(functions.lower(functions.regexp_replace(functions.col(column), "_", " ")).contains(fileName)).count() > 0
            );

            return !containsFileName;
        } catch (Exception e) {
            System.err.println("Error executing CSV Title In Distribution Check: " + e.getMessage());
            return false;
        }
    }
}
