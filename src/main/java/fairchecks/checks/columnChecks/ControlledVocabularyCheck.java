package fairchecks.checks.columnChecks;

import fairchecks.api.IInteroperabilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

public class ControlledVocabularyCheck implements IInteroperabilityCheck {
	
	private final String columnName;
    private final List<String> invalidRows = new ArrayList<>();
    private Set<String> allowedTerms = new HashSet<>();
    
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

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
    	try {
            String vocabularyURI = resolveVocabularyURI(columnName);

            if (vocabularyURI == null) {
                System.err.println("No vocabulary mapping found for column: " + columnName);
                return false;
            }

            allowedTerms = fetchControlledTermsFromSPARQL(vocabularyURI);

            List<Row> rows = dataset
                    .filter(functions.col(columnName).isNotNull())
                    .select(columnName)
                    .collectAsList();

            for (Row row : rows) {
                Object raw = row.get(0);
                if (raw == null) continue;

                String value = raw.toString().trim();
                if (value.equalsIgnoreCase("null")) continue;

                if (!allowedTerms.contains(value)) {
                	if (value.equalsIgnoreCase("null")) continue;
                    invalidRows.add("Invalid controlled term: " + value);
                }
            }

            return invalidRows.isEmpty();
        } catch (Exception e) {
            System.err.println("ControlledVocabularyCheck failed: " + e.getMessage());
            return false;
        }
    }

    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }

    private String resolveVocabularyURI(String columnName) {
        String key = columnName.toLowerCase().replaceAll("[^a-z]", "");

        Map<String, String> vocabularyMap = Map.ofEntries(
            Map.entry("currency", "http://publications.europa.eu/resource/authority/currency"),
            Map.entry("country", "http://publications.europa.eu/resource/authority/country"),
            Map.entry("language", "http://publications.europa.eu/resource/authority/language"),
            Map.entry("occupation", "http://publications.europa.eu/resource/authority/occupation"),
            Map.entry("role", "http://publications.europa.eu/resource/authority/role"),
            Map.entry("accessright", "http://publications.europa.eu/resource/authority/access-right"),
            Map.entry("filetype", "http://publications.europa.eu/resource/authority/file-type")
            //Add more mappings here as needed
        );
        
        return vocabularyMap.getOrDefault(key, null);
    }
    
    private Set<String> fetchControlledTermsFromSPARQL(String vocabulary) throws Exception {
        String endpoint = "https://publications.europa.eu/webapi/rdf/sparql";

        String query = String.format(
        	    "PREFIX skos: <http://www.w3.org/2004/02/skos/core#> " +
        	    "SELECT DISTINCT ?label WHERE { " +
        	    "?concept skos:inScheme <%s> ; " +
        	    "skos:prefLabel ?label . " +
        	    "FILTER (lang(?label) = \"en\") " +
        	    "} LIMIT 1000",
        	    vocabulary
        	);

        String fullUrl = endpoint + "?query=" + URLEncoder.encode(query, "UTF-8");

        HttpURLConnection conn = (HttpURLConnection) new URL(fullUrl).openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Accept", "application/sparql-results+json");
        conn.setRequestProperty("User-Agent", "Mozilla/5.0");

        int responseCode = conn.getResponseCode();
        if (responseCode != 200) {
            throw new RuntimeException("SPARQL request failed. HTTP error code: " + responseCode);
        }

        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String result = in.lines().collect(Collectors.joining());
        in.close();

        JSONObject json = new JSONObject(result);
        JSONArray bindings = json.getJSONObject("results").getJSONArray("bindings");

        Set<String> terms = new HashSet<>();
        for (int i = 0; i < bindings.length(); i++) {
            JSONObject label = bindings.getJSONObject(i).getJSONObject("label");
            terms.add(label.getString("value"));
        }

        return terms;
    }

}
