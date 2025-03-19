package fairchecks.checks.columnChecks;

import fairchecks.api.IInteroperabilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class ControlledVocabularyCheck implements IInteroperabilityCheck {
	
	private final String columnName;
    private HashSet<String> allowedTerms;
    private final List<String> invalidRows = new ArrayList<>();

    public ControlledVocabularyCheck(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String getCheckId() {
        return "IEU4";
    }

    @Override
    public String getCheckDescription() {
        return "Data shall clearly reuse concepts from controlled RDF vocabularies.";
    }

    @SuppressWarnings("unchecked")
	@Override
    public boolean executeCheck(Dataset<Row> dataset) {
    	allowedTerms = (HashSet<String>) dataset
            .groupBy(columnName)
            .count()
            .orderBy(functions.desc("count"))
            .limit(10)
            .select(columnName)
            .collectAsList()
            .stream()
            .map(row -> row.getString(0))
            .collect(Collectors.toList());

        List<Row> failingRows = dataset
                .filter(functions.col(columnName).isNotNull()
                        .and(functions.not(functions.col(columnName).isin(allowedTerms.toArray()))))
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
