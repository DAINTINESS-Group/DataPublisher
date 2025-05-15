package engine;

import fairchecks.api.IGenericCheck;
import fairchecks.factory.GlobalFairCheckFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Service class responsible for executing global FAIR checks on a dataset.
 * <p>
 * This class executes checks grouped under the FAIR principles:
 * Findability, Accessibility, Interoperability, and Reusability. It executes all global checks, or a specific one.
 * It does check instantiation to {@link GlobalFairCheckFactory} and runs each check sequentially on the full dataset.
 * </p>
 * 
 * @see fairchecks.factory.GlobalFairCheckFactory
 * @see fairchecks.api
 */
public class GlobalFairCheckService {
	
	public Map<String, Boolean> executeGlobalChecks(Dataset<Row> dataset) {
        Map<String, Boolean> results = new LinkedHashMap<>();

        for (IGenericCheck check : GlobalFairCheckFactory.getInteroperabilityChecks()) {
            results.put(check.getCheckId() + " - " + check.getCheckDescription(), check.executeCheck(dataset));
        }

        for (IGenericCheck check : GlobalFairCheckFactory.getAccessibilityChecks()) {
            results.put(check.getCheckId() + " - " + check.getCheckDescription(), check.executeCheck(dataset));
        }

        for (IGenericCheck check : GlobalFairCheckFactory.getFindabilityChecks()) {
            results.put(check.getCheckId() + " - " + check.getCheckDescription(), check.executeCheck(dataset));
        }
        
        for (IGenericCheck check : GlobalFairCheckFactory.getReusabilityChecks()) {
            results.put(check.getCheckId() + " - " + check.getCheckDescription(), check.executeCheck(dataset));
        }

        return results;
    }
	
	public Map<String, Boolean> executeGlobalCheckById(Dataset<Row> dataset, String checkId) {
		Map<String, Boolean> results = new LinkedHashMap<>();
		for (IGenericCheck check : GlobalFairCheckFactory.getAllGlobalChecks()) {
			if (check.getCheckId() == checkId) {
				results.put(check.getCheckId() + " - " + check.getCheckDescription(), check.executeCheck(dataset));
				break;
			}
		}
		if (results.isEmpty()) {
			throw new IllegalArgumentException("Check ID not found: " + checkId);
		}
		return results;
	}
}
