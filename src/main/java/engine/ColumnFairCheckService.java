package engine;


import fairchecks.factory.ColumnFairCheckFactory;
import fairchecks.api.*;
import model.DatasetProfile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnFairCheckService {
	
	//private ColumnFairCheckFactory columnCheckFactory = new ColumnFairCheckFactory();

	public Map<String, Map<String, Boolean>> executeColumnChecks(DatasetProfile profile) {
        Dataset<Row> dataset = profile.getDataset();
        String[] columnNames = dataset.columns();
        Map<String, Map<String, Boolean>> columnCheckResults = new HashMap<>();

        for (String columnName : columnNames) {
            Map<String, Boolean> checkResults = new HashMap<>();

            // Execute Findability Checks
            List<IFindabilityCheck> findabilityChecks = ColumnFairCheckFactory.getFindabilityChecks(columnName);
            for (IFindabilityCheck check : findabilityChecks) {
                boolean result = check.executeCheck(dataset);
                checkResults.put(check.getCheckId(), result);
            }

            // Execute Accessibility Checks
            List<IAccessibilityCheck> accessibilityChecks = ColumnFairCheckFactory.getAccessibilityChecks(columnName);
            for (IAccessibilityCheck check : accessibilityChecks) {
                boolean result = check.executeCheck(dataset);
                checkResults.put(check.getCheckId(), result);
            }

            // Execute Interoperability Checks
            List<IInteroperabilityCheck> interoperabilityChecks = ColumnFairCheckFactory.getInteroperabilityChecks(columnName);
            for (IInteroperabilityCheck check : interoperabilityChecks) {
                boolean result = check.executeCheck(dataset);
                checkResults.put(check.getCheckId(), result);
            }

            // Execute Reusability Checks
            List<IReusabilityCheck> reusabilityChecks = ColumnFairCheckFactory.getReusabilityChecks(columnName);
            for (IReusabilityCheck check : reusabilityChecks) {
                boolean result = check.executeCheck(dataset);
                checkResults.put(check.getCheckId(), result);
            }

            columnCheckResults.put(columnName, checkResults);
        }

        return columnCheckResults;
    }

}
