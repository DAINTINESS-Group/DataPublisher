package fairchecks.checks;

import fairchecks.api.IAccessibilityCheck;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataAccessRestrictionCheck implements IAccessibilityCheck {
	
	@Override
    public String getCheckId() {
        return "AEU1";
    }

    @Override
    public String getCheckDescription() {
        return "Data to be published without access restrictions.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
        try {
            String accessRestricted = dataset.sparkSession().conf().get("data.access.restricted", "false");

            return !accessRestricted.equalsIgnoreCase("true");
        } catch (Exception e) {
            System.err.println("Error executing Data Access Restriction Check: " + e.getMessage());
            return false;
        }
    }

}
