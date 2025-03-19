package fairchecks.checks.columnChecks;

import fairchecks.api.IInteroperabilityCheck;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class UnitInHeaderCheck implements IInteroperabilityCheck{
	
	private final String columnName;
    private final List<String> invalidRows = new ArrayList<>();

    public UnitInHeaderCheck(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String getCheckId() {
        return "IEU17.1";
    }

    @Override
    public String getCheckDescription() {
        return "The unit of a value shall be declared in the relevant column header.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
        boolean containsUnit = columnName.matches(".*\\(.*\\).*");

        if (!containsUnit) {
            invalidRows.add("Column '" + columnName + "' does not contain a unit in its header.");
        }

        return containsUnit;
    }

    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }

}
