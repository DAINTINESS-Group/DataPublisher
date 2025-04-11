package engine;

import model.FairCheckResult;
import fairchecks.api.IAccessibilityCheck;
import fairchecks.api.IFindabilityCheck;
import fairchecks.api.IInteroperabilityCheck;
import fairchecks.api.IReusabilityCheck;
import fairchecks.factory.ColumnFairCheckFactory;
//import fairchecks.api.*;
import model.DatasetProfile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnFairCheckService {
	
	//private ColumnFairCheckFactory columnCheckFactory = new ColumnFairCheckFactory();

	public Map<String, Map<String, List<FairCheckResult>>> executeColumnChecks(DatasetProfile profile) {
        Dataset<Row> dataset = profile.getDataset();
        String[] columnNames = dataset.columns();
        Map<String, Map<String, List<FairCheckResult>>> columnCheckResults = new HashMap<>();

        for (String columnName : columnNames) {
        	if (columnName.equalsIgnoreCase("_id")) continue;
        	Map<String, List<FairCheckResult>> categorizedResults = new HashMap<>();
        	
        	// Execute Findability Checks
        	List<IFindabilityCheck> findabilityChecks = ColumnFairCheckFactory.getFindabilityChecks(columnName);
        	List<FairCheckResult> findabilityResults = new ArrayList<>();
        	for (IFindabilityCheck check : findabilityChecks) {
        	    boolean result = check.executeCheck(dataset);
        	    List<String> invalidRows = check.getInvalidRows();
        	    findabilityResults.add(new FairCheckResult(check.getCheckId(), check.getCheckDescription(), result, invalidRows));

        	    System.out.println("[" + columnName + "] " + check.getCheckId() + ": " + (result ? "PASSED" : "FAILED"));
        	}
        	categorizedResults.put("Findability", findabilityResults);

            // Execute Accessibility Checks
        	List<IAccessibilityCheck> accessibilityChecks = ColumnFairCheckFactory.getAccessibilityChecks(columnName);
        	List<FairCheckResult> accessibilityResults = new ArrayList<>();
        	for (IAccessibilityCheck check : accessibilityChecks) {
        	    boolean result = check.executeCheck(dataset);
        	    List<String> invalidRows = check.getInvalidRows();
        	    accessibilityResults.add(new FairCheckResult(check.getCheckId(), check.getCheckDescription(), result, invalidRows));

        	    System.out.println("[" + columnName + "] " + check.getCheckId() + ": " + (result ? "PASSED" : "FAILED"));
        	}
        	categorizedResults.put("Accessibility", accessibilityResults);


            // Execute Interoperability Checks
        	List<IInteroperabilityCheck> interoperabilityChecks = ColumnFairCheckFactory.getInteroperabilityChecks(columnName);
        	List<FairCheckResult> interoperabilityResults = new ArrayList<>();
        	for (IInteroperabilityCheck check : interoperabilityChecks) {
        	    boolean result = check.executeCheck(dataset);
        	    List<String> invalidRows = check.getInvalidRows();
        	    interoperabilityResults.add(new FairCheckResult(check.getCheckId(), check.getCheckDescription(), result, invalidRows));

        	    System.out.println("[" + columnName + "] " + check.getCheckId() + ": " + (result ? "PASSED" : "FAILED"));
        	}
        	categorizedResults.put("Interoperability", interoperabilityResults);


            // Execute Reusability Checks
        	List<IReusabilityCheck> reusabilityChecks = ColumnFairCheckFactory.getReusabilityChecks(columnName);
        	List<FairCheckResult> reusabilityResults = new ArrayList<>();
        	for (IReusabilityCheck check : reusabilityChecks) {
        	    boolean result = check.executeCheck(dataset);
        	    List<String> invalidRows = check.getInvalidRows();
        	    reusabilityResults.add(new FairCheckResult(check.getCheckId(), check.getCheckDescription(), result, invalidRows));

        	    System.out.println("[" + columnName + "] " + check.getCheckId() + ": " + (result ? "PASSED" : "FAILED"));
        	}
        	categorizedResults.put("Reusability", reusabilityResults);


            columnCheckResults.put(columnName, categorizedResults);
        }

        return columnCheckResults;
    }

}
