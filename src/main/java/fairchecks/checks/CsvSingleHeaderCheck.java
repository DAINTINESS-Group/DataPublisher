package fairchecks.checks;

import fairchecks.api.IInteroperabilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

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
        	String delimiter = dataset.sparkSession().conf().get("spark.sql.csv.delimiter", ";");
            
            String[] columns = dataset.columns();
            long duplicateHeaderRows = dataset.filter(
                functions.concat_ws(delimiter, functions.col(columns[0]), functions.col(columns[1]))
                .equalTo(functions.concat_ws(delimiter, functions.lit(columns[0]), functions.lit(columns[1])))
            ).count();

            return duplicateHeaderRows <= 1;
        } catch (Exception e) {
            System.err.println("Error executing CSV Single Header Check: " + e.getMessage());
            return false;
        }
    }

}
