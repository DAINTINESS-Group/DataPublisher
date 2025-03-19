package fairchecks.checks;

import fairchecks.api.IInteroperabilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class UriDetailsRetrievalCheck implements IInteroperabilityCheck{
	
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
        try {
            long uriCount = dataset.filter(functions.col("uri").rlike("^https?://")).count();
            
            return uriCount > 0;
        } catch (Exception e) {
            System.err.println("Error executing URI Details Retrieval Check: " + e.getMessage());
            return false;
        }
    }

}
