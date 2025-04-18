package engine;

import model.FairCheckResult;
import fairchecks.api.IAccessibilityCheck;
import fairchecks.api.IFindabilityCheck;
import fairchecks.api.IInteroperabilityCheck;
import fairchecks.api.IReusabilityCheck;
import fairchecks.factory.ColumnFairCheckFactory;
import model.DatasetProfile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service class responsible for executing all applicable FAIR checks on each column of a dataset.
 * <p>
 * This service executes checks across the four FAIR categories:
 * Findability, Accessibility, Interoperability, and Reusability. It uses the
 * {@link fairchecks.factory.ColumnFairCheckFactory} to obtain relevant checks for each column,
 * and evaluates their applicability based on column data types.
 * </p>
 *
 * <p>
 * The results are grouped by column and by FAIR category, and returned as structured
 * {@link model.FairCheckResult} objects for use in reporting or further analysis.
 * </p>
 * @see model.FairCheckResult
 * @see fairchecks.factory.ColumnFairCheckFactory
 * @see fairchecks.api
 */
public class ColumnFairCheckService {
	
	public Map<String, Map<String, List<FairCheckResult>>> executeColumnChecks(DatasetProfile profile) {
        Dataset<Row> dataset = profile.getDataset();
        String[] columnNames = dataset.columns();
        Map<String, Map<String, List<FairCheckResult>>> columnCheckResults = new HashMap<>();

        for (String columnName : columnNames) {
        	if (columnName.equalsIgnoreCase("_id")) continue;
        	
        	DataType columnType = dataset.schema().apply(columnName).dataType();
        	Map<String, List<FairCheckResult>> categorizedResults = new HashMap<>();
        	
        	// Execute Findability Checks
        	List<IFindabilityCheck> findabilityChecks = ColumnFairCheckFactory.getFindabilityChecks(columnName);
        	List<FairCheckResult> findabilityResults = new ArrayList<>();
        	for (IFindabilityCheck check : findabilityChecks) {
        		if (check.isApplicable(columnType)) {
	        	    boolean result = check.executeCheck(dataset);
	        	    List<String> invalidRows = check.getInvalidRows();
	        	    findabilityResults.add(new FairCheckResult(check.getCheckId(), check.getCheckDescription(), result, invalidRows));
        		}
        	}
        	categorizedResults.put("Findability", findabilityResults);

            // Execute Accessibility Checks
        	List<IAccessibilityCheck> accessibilityChecks = ColumnFairCheckFactory.getAccessibilityChecks(columnName);
        	List<FairCheckResult> accessibilityResults = new ArrayList<>();
        	for (IAccessibilityCheck check : accessibilityChecks) {
        		if (check.isApplicable(columnType)) {
	        	    boolean result = check.executeCheck(dataset);
	        	    List<String> invalidRows = check.getInvalidRows();
	        	    accessibilityResults.add(new FairCheckResult(check.getCheckId(), check.getCheckDescription(), result, invalidRows));
	        	}
        	}
        	categorizedResults.put("Accessibility", accessibilityResults);

            // Execute Interoperability Checks
        	List<IInteroperabilityCheck> interoperabilityChecks = ColumnFairCheckFactory.getInteroperabilityChecks(columnName);
        	List<FairCheckResult> interoperabilityResults = new ArrayList<>();
        	for (IInteroperabilityCheck check : interoperabilityChecks) {
        		if (check.isApplicable(columnType)) {
	        	    boolean result = check.executeCheck(dataset);
	        	    List<String> invalidRows = check.getInvalidRows();
	        	    interoperabilityResults.add(new FairCheckResult(check.getCheckId(), check.getCheckDescription(), result, invalidRows));
	        	}
        	}
        	categorizedResults.put("Interoperability", interoperabilityResults);

            // Execute Reusability Checks
        	List<IReusabilityCheck> reusabilityChecks = ColumnFairCheckFactory.getReusabilityChecks(columnName);
        	List<FairCheckResult> reusabilityResults = new ArrayList<>();
        	for (IReusabilityCheck check : reusabilityChecks) {
        		if (check.isApplicable(columnType)) {
	        	    boolean result = check.executeCheck(dataset);
	        	    List<String> invalidRows = check.getInvalidRows();
	        	    reusabilityResults.add(new FairCheckResult(check.getCheckId(), check.getCheckDescription(), result, invalidRows));
	        	}
        	}
        	categorizedResults.put("Reusability", reusabilityResults);

            columnCheckResults.put(columnName, categorizedResults);
        }

        return columnCheckResults;
    }
}
