package fairchecks.checks.globalChecks;

import fairchecks.api.IGenericCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.*;
import java.util.regex.Pattern;

/**
 * A global check that ensures each column containing URIs from controlled vocabularies
 * is accompanied by a corresponding column that provides human-readable labels.
 *
 * <p>Check ID: IEU6
 */
public class UriDetailsRetrievalCheck implements IGenericCheck{
	
	private static final Pattern uriPattern = Pattern.compile("^(https?|ftp)://[\\w.-]+(/[\\w\\-./]*)?$");

    private static final String[] knownVocabBases = new String[]{
        "http://publications.europa.eu/resource/authority/measurement-unit/",
        "http://publications.europa.eu/resource/authority/country/",
        "http://publications.europa.eu/resource/authority/currency/"
    };

    private final List<String> invalidRows = new ArrayList<>();
	
	@Override
    public String getCheckId() {
        return "IEU6";
    }

    @Override
    public String getCheckDescription() {
        return "Once tags are referenced by unique identifiers from controlled vocabularies, URI details shall be retrieved in the data.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
    	String[] columns = dataset.columns();
        Map<String, List<String>> candidatePairs = new HashMap<>();
        boolean foundControlledURIs = false;

        for (String column : columns) {
            List<Row> values = dataset.select(column)
                    .filter(functions.col(column).isNotNull())
                    .collectAsList();

            if (values.isEmpty()) continue;

            long matchCount = values.stream()
                    .map(row -> row.get(0).toString().trim())
                    .filter(this::isControlledVocabularyURI)
                    .count();

            double ratio = (double) matchCount / values.size();

            if (ratio >= 0.5) {
            	foundControlledURIs = true;
                String labelColumn = findMatchingLabelColumn(column, columns);
                if (labelColumn == null) {
                    invalidRows.add("No label column found for controlled URI column: " + column);
                } else {
                    candidatePairs.put(column, Arrays.asList(labelColumn));
                }
            }
        }
        
        if(!foundControlledURIs) {
        	return false;
        }

        return invalidRows.isEmpty();
    }
    
    private String findMatchingLabelColumn(String uriColumn, String[] allColumns) {
        String uriColNormalized = uriColumn.toLowerCase().replaceAll("[^a-z]", "");

        for (String otherCol : allColumns) {
            if (otherCol.equals(uriColumn)) continue;

            String labelColNormalized = otherCol.toLowerCase().replaceAll("[^a-z]", "");

            if (labelColNormalized.startsWith(uriColNormalized.replace("uri", ""))
                    || labelColNormalized.contains("label")) {
                return otherCol;
            }
        }

        return null;
    }

    private boolean isControlledVocabularyURI(String value) {
        if (!uriPattern.matcher(value).matches()) return false;
        for (String base : knownVocabBases) {
            if (value.startsWith(base)) return true;
        }
        return false;
    }
}
