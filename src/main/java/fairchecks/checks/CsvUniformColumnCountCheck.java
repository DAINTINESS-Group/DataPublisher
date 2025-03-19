package fairchecks.checks;

import fairchecks.api.IInteroperabilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

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
        	String delimiter = dataset.sparkSession().conf().get("spark.sql.csv.delimiter", ";");
        	
        	long uniqueColumnCounts = dataset.selectExpr(
                    "size(split(concat_ws('" + delimiter + "', *), '" + delimiter + "')) as colCount"
                )
                .agg(functions.countDistinct("colCount"))
                .first().getLong(0);

            return uniqueColumnCounts == 1;
        } catch (Exception e) {
            System.err.println("Error executing CSV Uniform Column Count Check: " + e.getMessage());
            return false;
        }
    }

}
