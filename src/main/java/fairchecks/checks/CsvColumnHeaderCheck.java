package fairchecks.checks;

import fairchecks.api.IInteroperabilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.Arrays;

public class CsvColumnHeaderCheck implements IInteroperabilityCheck{
	
	@Override
    public String getCheckId() {
        return "IEU15";
    }

    @Override
    public String getCheckDescription() {
        return "CSV - Data files should contain column headers as their first row.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
        try {
            String delimiter = dataset.sparkSession().conf().get("spark.sql.csv.delimiter", ";");

            org.apache.spark.sql.Column[] columns = Arrays.stream(dataset.columns())
                    .map(functions::col)
                    .toArray(org.apache.spark.sql.Column[]::new);

            long headerRowCount = dataset.limit(1).filter(
                functions.concat_ws(delimiter, columns)
                .equalTo(functions.concat_ws(delimiter, Arrays.stream(dataset.columns())
                    .map(functions::lit)
                    .toArray(org.apache.spark.sql.Column[]::new)))
            ).count();

            return headerRowCount > 0;
        } catch (Exception e) {
            System.err.println("Error executing CSV Column Header Check: " + e.getMessage());
            return false;
        }
    }

}
