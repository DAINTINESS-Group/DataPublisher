package engine;

import fairchecks.api.*;
import fairchecks.factory.GlobalFairCheckFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.LinkedHashMap;
import java.util.Map;

public class GlobalFairCheckService {
	
	public Map<String, Boolean> executeGlobalChecks(Dataset<Row> dataset) {
        Map<String, Boolean> results = new LinkedHashMap<>();

        for (IInteroperabilityCheck check : GlobalFairCheckFactory.getInteroperabilityChecks()) {
            results.put(check.getCheckId() + " - " + check.getCheckDescription(), check.executeCheck(dataset));
        }

        for (IAccessibilityCheck check : GlobalFairCheckFactory.getAccessibilityChecks()) {
            results.put(check.getCheckId() + " - " + check.getCheckDescription(), check.executeCheck(dataset));
        }

        for (IFindabilityCheck check : GlobalFairCheckFactory.getFindabilityChecks()) {
            results.put(check.getCheckId() + " - " + check.getCheckDescription(), check.executeCheck(dataset));
        }
        
        for (IReusabilityCheck check : GlobalFairCheckFactory.getReusabilityChecks()) {
            results.put(check.getCheckId() + " - " + check.getCheckDescription(), check.executeCheck(dataset));
        }

        return results;
    }

}
