package fairchecks.checks;

import fairchecks.api.IInteroperabilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class UriLinkedDataCheck implements IInteroperabilityCheck{
	
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
        try {
            long linkedDataUriCount = dataset.filter(
                    functions.col("uri").rlike("^(https?|ftp)://.+(/rdf|/json-ld|/ontology|/vocab)").or(
                            functions.col("uri").rlike(".*wikidata.*")
                    )).count();
            
            return linkedDataUriCount > 0;
        } catch (Exception e) {
            System.err.println("Error executing URI Linked Data Check: " + e.getMessage());
            return false;
        }
    }

}
