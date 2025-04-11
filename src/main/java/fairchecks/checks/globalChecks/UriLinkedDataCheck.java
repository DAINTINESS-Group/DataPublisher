package fairchecks.checks.globalChecks;

import fairchecks.api.IInteroperabilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class UriLinkedDataCheck implements IInteroperabilityCheck{
	
	private static final Pattern uriPattern = Pattern.compile("^(https?|ftp)://.+$");
    private final List<String> invalidRows = new ArrayList<>();
	
	@Override
    public String getCheckId() {
        return "IEU7";
    }

    @Override
    public String getCheckDescription() {
        return "Data shall leverage URIs and be published as Linked Data in the form of semantic characters.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
    	String[] columns = dataset.columns();

    	boolean hasSubjectUriColumn = false;
        boolean hasObjectUriColumn = false;

        for (String column : columns) {
            List<Row> values = dataset.select(column)
                    .filter(functions.col(column).isNotNull())
                    .limit(20)
                    .collectAsList();

            if (values.isEmpty()) continue;

            long uriCount = values.stream()
                    .map(row -> row.get(0).toString().trim())
                    .filter(val -> uriPattern.matcher(val).matches())
                    .count();

            double ratio = (double) uriCount / values.size();

            String normalized = column.toLowerCase();
            if ((normalized.equals("@id") || normalized.equals("id") || normalized.contains("subject") || normalized.contains("identifier"))
                && values.size() >= 3 && ratio >= 0.5) {
                hasSubjectUriColumn = true;
            }
            else if (values.size() >= 3 && ratio >= 0.5) {
                hasObjectUriColumn = true;
            }
            
            //System.out.println("Subject URI column match: " + column + " → " + (hasSubjectUriColumn ? "YES" : "NO"));

        }

        if (!hasSubjectUriColumn) {
            invalidRows.add("Missing subject URI column (e.g., '@id' or column containing URIs identifying the subject).");
        }
        if (!hasObjectUriColumn) {
            invalidRows.add("No object URI column found (i.e., no values linking to external vocabularies or resources).");
        }

        return hasSubjectUriColumn && hasObjectUriColumn;
    }

}
