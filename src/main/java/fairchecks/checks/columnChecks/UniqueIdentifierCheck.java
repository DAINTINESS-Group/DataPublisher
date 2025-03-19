package fairchecks.checks.columnChecks;

import fairchecks.api.IInteroperabilityCheck;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class UniqueIdentifierCheck implements IInteroperabilityCheck {
	
	private final String columnName;
	private final List<String> invalidRows = new ArrayList<>();

    public UniqueIdentifierCheck(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String getCheckId() {
        return "IEU5";
    }

    @Override
    public String getCheckDescription() {
        return "Data labels shall be referenced by unique identifiers, i.e. URIs";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
    	String uriRegex = "^(https?|ftp)://[a-zA-Z0-9\\-_.]+\\.[a-zA-Z]{2,}(/\\S*)?$";

        List<Row> failingRows = dataset
                .filter(functions.col(columnName).isNotNull()
                        .and(functions.not(functions.col(columnName).rlike(uriRegex))))
                .select(columnName)
                .collectAsList();

        for (Row row : failingRows) {
            invalidRows.add(row.getString(0));
        }

        return invalidRows.isEmpty();
    }
    
    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }

}
