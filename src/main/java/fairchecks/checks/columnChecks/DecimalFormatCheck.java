package fairchecks.checks.columnChecks;

import fairchecks.api.IGenericColumnCheck;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;

/**
 * A check that ensures numeric values in a column use a dot (.) to separate
 * integers from decimals, in compliance with standard decimal notation.
 *
 * <p>Applicable to numeric column types including integer, long, float, double, and decimal.
 *
 * <p>Check ID: IEU2.1
 */
public class DecimalFormatCheck implements IGenericColumnCheck {
	
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
                .select(functions.col("_id"), functions.col(columnName))
                .collectAsList();

        for (Row row : candidateRows) {
        	Number rowIdNum = (Number) row.getAs("_id");
        	long rowId = rowIdNum.longValue() + 1;
        	
        	Object rawVal = row.get(1);
        	if (rawVal == null) continue;
        	
        	String value = rawVal.toString().trim();

            if (value.equalsIgnoreCase("null")) continue;

            if (!value.matches(decimalRegex)) {
                invalidRows.add("Row " + rowId + ": Invalid decimal: " + value);
            }
        }


        return invalidRows.isEmpty();
    }
    
    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }
    
    @Override
    public boolean isApplicable(DataType columnType) {
        return columnType instanceof IntegerType ||
               columnType instanceof LongType ||
               columnType instanceof DoubleType ||
               columnType instanceof FloatType ||
               columnType instanceof DecimalType;
    }
}
