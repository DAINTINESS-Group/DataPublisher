package fairchecks.checks;

import fairchecks.api.IInteroperabilityCheck;

import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

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
        	String delimiter = dataset.sparkSession().conf().get("spark.sql.csv.delimiter", ";");
        	
        	org.apache.spark.sql.Column[] columns = Arrays.stream(dataset.columns())
                    .map(functions::col)
                    .toArray(org.apache.spark.sql.Column[]::new);
            
        	String fileName = dataset.sparkSession().conf().get("spark.sql.csv.filepath", "").toLowerCase();

            long titleRowCount = dataset.limit(1).filter(
                functions.lower(functions.concat_ws(delimiter, columns))
                .rlike(".*" + fileName.replace("_", " ").replace(".csv", "") + ".*")
            ).count();

            return titleRowCount == 0;
        } catch (Exception e) {
            System.err.println("Error executing CSV Title In Distribution Check: " + e.getMessage());
            return false;
        }
    }

}
