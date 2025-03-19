package fairchecks.checks.columnChecks;

import fairchecks.api.IInteroperabilityCheck;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

//TODO maybe global or change method?

public class UnitInDedicatedColumnCheck implements IInteroperabilityCheck {
	
	private final String columnName;
    private final List<String> invalidRows = new ArrayList<>();

    public UnitInDedicatedColumnCheck(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String getCheckId() {
        return "IEU17.3";
    }

    @Override
    public String getCheckDescription() {
        return "If the unit varies, the files shall have a special column for it.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
        StructType schema = dataset.schema();
        boolean hasUnitColumn = false;

        for (StructField field : schema.fields()) {
            String colName = field.name().toLowerCase();
            if (colName.contains("unit")) {
                hasUnitColumn = true;
                break;
            }
        }

        if (!hasUnitColumn) {
            invalidRows.add("No dedicated column found for units in dataset.");
        }

        return hasUnitColumn;
    }

    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }

}
