package engine;

import fairchecks.api.*;
import fairchecks.factory.FairCheckFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.LinkedHashMap;
import java.util.Map;

public class GlobalFairCheckService {
	
	public Map<String, Boolean> executeGlobalChecks(Dataset<Row> dataset) {
        Map<String, Boolean> results = new LinkedHashMap<>();

        for (IInteroperabilityCheck check : FairCheckFactory.getInteroperabilityChecks()) {
            results.put(check.getCheckId() + " - " + check.getCheckDescription(), check.executeCheck(dataset));
        }

        for (IAccessibilityCheck check : FairCheckFactory.getAccessibilityChecks()) {
            results.put(check.getCheckId() + " - " + check.getCheckDescription(), check.executeCheck(dataset));
        }

        for (IFindabilityCheck check : FairCheckFactory.getFindabilityChecks()) {
            results.put(check.getCheckId() + " - " + check.getCheckDescription(), check.executeCheck(dataset));
        }
        
        for (IReusabilityCheck check : FairCheckFactory.getReusabilityChecks()) {
            results.put(check.getCheckId() + " - " + check.getCheckDescription(), check.executeCheck(dataset));
        }

        return results;
    }

}
