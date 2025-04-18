package engine;

import fairchecks.api.IAccessibilityCheck;
import fairchecks.api.IFindabilityCheck;
import fairchecks.api.IInteroperabilityCheck;
import fairchecks.api.IReusabilityCheck;
import fairchecks.factory.GlobalFairCheckFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Service class responsible for executing all global FAIR checks on a dataset.
 * <p>
 * This class executes checks grouped under the FAIR principles:
 * Findability, Accessibility, Interoperability, and Reusability. It does check instantiation
 * to {@link GlobalFairCheckFactory} and runs each check sequentially on the full dataset.
 * </p>
 * @see fairchecks.factory.GlobalFairCheckFactory
 * @see fairchecks.api
 */
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
