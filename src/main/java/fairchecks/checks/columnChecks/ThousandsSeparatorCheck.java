package fairchecks.checks.columnChecks;

import fairchecks.api.IInteroperabilityCheck;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class ThousandsSeparatorCheck implements IInteroperabilityCheck  {
	
	private final String columnName;
	private final List<String> invalidRows = new ArrayList<>();

    public ThousandsSeparatorCheck(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String getCheckId() {
        return "IEU2.2";
    }

    @Override
    public String getCheckDescription() {
        return "Thousands must not use separators.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {    	
    	String thousandsSeparatorRegex = "^[0-9]{1,3}([.,\\s][0-9]{3})+$";

        List<Row> failingRows = dataset
                .filter(functions.col(columnName).isNotNull()
                        .and(functions.col(columnName).rlike(thousandsSeparatorRegex)))
                .select(columnName)
                .collectAsList();

        for (Row row : failingRows) {
        	Object rawVal = row.get(0);
            if (rawVal == null) continue;

            String value = rawVal.toString().trim();

            if (value.equalsIgnoreCase("null")) continue;
            
            invalidRows.add("Invalid format of number: " + value);
        }

        return invalidRows.isEmpty();
    }
    
    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }

}
