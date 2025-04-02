package fairchecks.checks.columnChecks;

import fairchecks.api.IInteroperabilityCheck;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CsvUnitInHeaderCheck implements IInteroperabilityCheck{
	
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

}
