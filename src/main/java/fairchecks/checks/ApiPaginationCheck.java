package fairchecks.checks;

import fairchecks.api.IReusabilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ApiPaginationCheck implements IReusabilityCheck{
	
	@Override
    public String getCheckId() {
        return "REU13";
    }

    @Override
    public String getCheckDescription() {
        return "API - Large data should be paginated, using offset and bounds parameters.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
        try {
            long rowCount = dataset.count();

            String paginationEnabled = dataset.sparkSession().conf().get("api.pagination.enabled", "false");
            boolean isPaginationSet = paginationEnabled.equalsIgnoreCase("true");

            return rowCount <= 1000 || isPaginationSet;
        } catch (Exception e) {
            System.err.println("Error executing API Pagination Check: " + e.getMessage());
            return false;
        }
    }

}
