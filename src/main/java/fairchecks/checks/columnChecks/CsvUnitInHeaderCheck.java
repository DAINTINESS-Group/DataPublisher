package fairchecks.checks.columnChecks;

import fairchecks.api.IGenericColumnCheck;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;

/**
 * A check that ensures numeric columns declare their measurement unit
 * explicitly in the column header, promoting clarity and semantic interoperability.
 * <p>Applicable only to numeric column types (int, long, float, double, decimal).
 *
 * <p>Check ID: IEU17.1
 */
public class CsvUnitInHeaderCheck implements IGenericColumnCheck {
	
	private final String columnName;
    private final List<String> invalidRows = new ArrayList<>();
    
    private static final List<String> COMMON_UNITS = Arrays.asList(
            "kg", "g", "mg", "lb", "oz", // Weight
            "m", "cm", "mm", "inch", "ft", // Length
            "l", "ml", "gal", "fl oz", // Volume
            "€", "$", "£", "¥", "₹", // Currency
            "°C", "°F", "K", // Temperature
            "s", "min", "h", "day", "week", "month", "year", // Time
            "%", "‰", "mol", "A", "W", "J" // Miscellaneous
        );
    
    public CsvUnitInHeaderCheck(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String getCheckId() {
        return "IEU17.1";
    }

    @Override
    public String getCheckDescription() {
        return " CSV - The unit of a value shall be declared in the relevant column header.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {        
    	String matchedColumn = null;
        for (String col : dataset.columns()) {
            if (col.equalsIgnoreCase(columnName) || col.toLowerCase().startsWith(columnName.toLowerCase())) {
                matchedColumn = col;
                break;
            }
        }

        if (matchedColumn == null) {
            invalidRows.add("Column '" + columnName + "' not found in dataset.");
            return false;
        }

        boolean hasUnit = containsUnit(matchedColumn);

        if (!hasUnit) {
            invalidRows.add("Column '" + matchedColumn + "' does not contain a unit in its header.");
        }

        return hasUnit;
    }

    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }
    
    private boolean containsUnit(String colName) {
        for (String unit : COMMON_UNITS) {
            if (colName.toLowerCase().matches(".*\\(" + unit.toLowerCase() + "\\).*") ||
                colName.toLowerCase().endsWith("_" + unit.toLowerCase())) {
                return true;
            }
        }

        return false;
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
