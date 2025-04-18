package fairchecks.checks.globalChecks;

import fairchecks.api.IInteroperabilityCheck;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * A global check that ensures dataset values are referenced using unique identifiers,
 * specifically URIs, to support interoperability.
 *
 * <p>Check ID: IEU5
 */
public class UniqueIdentifierCheck implements IInteroperabilityCheck {
	
	private final List<String> invalidRows = new ArrayList<>();
    private static final Pattern uriPattern = Pattern.compile("^(https?|ftp)://[\\w.-]+(/[\\w\\-./]*)?$");
    private static final double MATCH_THRESHOLD = 0.5;
    
    String[] knownBases = new String[] {
    	    "http://publications.europa.eu/resource/authority/",
    	    "https://publications.europa.eu/resource/authority/"
    	};

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
    	String[] columns = dataset.columns();
        boolean foundUriColumn = false;

        for (String column : columns) {
            List<Row> values = dataset.select(column).filter(functions.col(column).isNotNull()).collectAsList();

            if (values.isEmpty()) continue;

            long matchCount = values.stream()
                    .map(row -> row.get(0).toString().trim())
                    .filter(val -> uriPattern.matcher(val).matches())
                    .count();

            double matchRatio = (double) matchCount / values.size();

            if (matchRatio >= MATCH_THRESHOLD) {
                foundUriColumn = true;

                for (Row row : values) {
                	String val = row.get(0).toString().trim();
                    if (val.equalsIgnoreCase("null")) continue;

                    if (!uriPattern.matcher(val).matches()) {
                        invalidRows.add("Invalid URI in column '" + column + "': " + val);
                    } else if (!isKnownVocabularyUri(val)) {
                        System.out.println("Warning: URI is valid but not from a known controlled vocabulary: " + val);
                    }
                }
                break;
            }
        }

        return foundUriColumn && invalidRows.isEmpty();
    }
    
    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }
    
    boolean isKnownVocabularyUri(String uri) {
        for (String base : knownBases) {
            if (uri.startsWith(base)) return true;
        }
        return false;
    }
}
