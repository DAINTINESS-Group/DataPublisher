package fairchecks.checks.columnChecks;

import fairchecks.api.IInteroperabilityCheck;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class DecimalFormatCheck implements IInteroperabilityCheck {
	
	private final String columnName;
	private final List<String> invalidRows = new ArrayList<>();

    public DecimalFormatCheck(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String getCheckId() {
        return "IEU2.1";
    }

    @Override
    public String getCheckDescription() {
        return "Integers are separated from decimals by a dot “.”.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
    	String decimalRegex = "^-?\\d+(\\.\\d+)?$";

    	List<Row> candidateRows = dataset
                .filter(functions.col(columnName).isNotNull())
                .select(columnName)
                .collectAsList();

        for (Row row : candidateRows) {
            Object rawVal = row.get(0);
            if (rawVal == null) continue;

            String value = rawVal.toString().trim();

            if (value.equalsIgnoreCase("null")) continue;

            if (!value.matches(decimalRegex)) {
                invalidRows.add("Invalid decimal: " + value);
            }
        }


        return invalidRows.isEmpty();
    }
    
    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }

}
