package fairchecks.checks;

import fairchecks.api.IInteroperabilityCheck;

import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class SingleTableCheck implements IInteroperabilityCheck{
	
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
            String delimiter = dataset.sparkSession().conf().get("spark.sql.csv.delimiter", ";");
            
            org.apache.spark.sql.Column[] columns = Arrays.stream(dataset.columns())
                    .map(functions::col)
                    .toArray(org.apache.spark.sql.Column[]::new);
            
            long distinctHeaderCount = dataset.limit(100)
                    .select(functions.concat_ws(delimiter, columns).alias("row_signature"))
                    .agg(functions.countDistinct("row_signature"))
                    .first().getLong(0);

            return distinctHeaderCount == 1;
        } catch (Exception e) {
            System.err.println("Error executing Single Table Check: " + e.getMessage());
            return false;
        }
    }

}
