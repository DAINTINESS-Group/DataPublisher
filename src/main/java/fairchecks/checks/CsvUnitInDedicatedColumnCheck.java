package fairchecks.checks;

import fairchecks.api.IInteroperabilityCheck;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CsvUnitInDedicatedColumnCheck implements IInteroperabilityCheck {
	
	private final List<String> invalidRows = new ArrayList<>();

    @Override
    public String getCheckId() {
        return "IEU17.3";
    }

    @Override
    public String getCheckDescription() {
        return "If the unit varies within a column, the dataset shall have a special column for it.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
        StructType schema = dataset.schema();
        HashMap<String, HashSet<String>> columnUnitMap = new HashMap<>();
        HashSet<String> unitColumns = new HashSet<>();

        for (StructField field : schema.fields()) {
            String colName = field.name().toLowerCase();
            if (colName.contains("unit")) {
                unitColumns.add(colName);
            } else {
                columnUnitMap.put(colName, new HashSet<>());
            }
        }

        for (String colName : columnUnitMap.keySet()) {
            List<Row> inferredUnits = dataset
                .select(functions.regexp_extract(functions.col(colName), getUnitRegex(), 1).alias("detectedUnit"))
                .where(functions.col("detectedUnit").isNotNull().and(functions.col("detectedUnit").notEqual("")))
                .distinct()
                .collectAsList();
            
            for (Row row : inferredUnits) {
                columnUnitMap.get(colName).add(row.getString(0));
            }
        }

        for (String colName : columnUnitMap.keySet()) {
            HashSet<String> uniqueUnits = columnUnitMap.get(colName);

            if (uniqueUnits.size() > 1) {
                boolean hasDedicatedUnitColumn = unitColumns.contains(colName + "_unit") || unitColumns.contains("unit_" + colName);
                
                if (!hasDedicatedUnitColumn) {
                    invalidRows.add("Column '" + colName + "' has multiple units " + uniqueUnits + " but no dedicated unit column.");
                }
            }
        }

        return invalidRows.isEmpty();
    }

    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }
    
    private String getUnitRegex() {
        return "(?i)(%|‰|kg|g|mg|lb|oz|m|cm|mm|inch|ft|l|ml|gal|fl oz|€|$|£|¥|₹|°C|°F|K|s|min|h|day|week|month|year|mol|A|W|J)";
    }

}
