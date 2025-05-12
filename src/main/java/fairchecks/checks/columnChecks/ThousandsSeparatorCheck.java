package fairchecks.checks.columnChecks;

import fairchecks.api.IGenericColumnCheck;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;

/**
 * A check that ensures numeric values do not use thousands separators 
 * such as commas, dots, or spaces (e.g., "1,000" or "1 000").
 * <p>Applicable to all standard numeric types including integers, decimals, and floats.
 *
 * <p>Check ID: IEU2.2
 */
public class ThousandsSeparatorCheck implements IGenericColumnCheck  {
	
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
                .select(functions.col("_id"), functions.col(columnName))
                .collectAsList();

        for (Row row : failingRows) {
        	Number rowIdNum = (Number) row.getAs("_id");
        	long rowId = rowIdNum.longValue() + 1;
        	
        	Object raw = row.get(1);
        	String value = (raw == null) ? "<null>" : raw.toString();
        	invalidRows.add("Row " + rowId + ": Invalid thousands saparator in value: " + value);
        }

        return invalidRows.isEmpty();
    }
    
    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }
    
    @Override
    public boolean isApplicable(DataType columnType) {
        return columnType.equals(DataTypes.IntegerType) ||
               columnType.equals(DataTypes.LongType) ||
               columnType.equals(DataTypes.DoubleType) ||
               columnType.equals(DataTypes.FloatType) ||
               columnType instanceof DecimalType;
    }
}
