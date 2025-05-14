package engine;

import model.FairCheckResult;
import fairchecks.api.IGenericCheck;
import fairchecks.api.IGenericColumnCheck;
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
        	
        	columnCheckResults = executeAllColumnFairChecks(dataset, columnName, columnCheckResults);
        }

        return columnCheckResults;
    }
	
	public Map<String, Map<String, List<FairCheckResult>>> executeColumnCheckById(DatasetProfile profile, String checkId) {
		Dataset<Row> dataset = profile.getDataset();
        String[] columnNames = dataset.columns();
        Map<String, Map<String, List<FairCheckResult>>> columnCheckResults = new HashMap<>();
        
        String fairCategory = getFairCategory(checkId);
        
        
        
        for (String columnName : columnNames) {
        	if (columnName.equalsIgnoreCase("_id")) continue;
        	
        	Map<String, List<FairCheckResult>> categorizedResults = new HashMap<>();
        	List<FairCheckResult> checkResults = new ArrayList<>();
        	
        	List<IGenericCheck> columnChecks = ColumnFairCheckFactory.getAllColumnChecks(columnName);

        	for (IGenericCheck check : columnChecks) {
        		if (check.getCheckId().equalsIgnoreCase(checkId)) {
        			boolean result = check.executeCheck(dataset);
	        	    List<String> invalidRows = ((IGenericColumnCheck) check).getInvalidRows();
        			checkResults.add(new FairCheckResult(check.getCheckId(), check.getCheckDescription(), result, invalidRows));
        			break;
        		}
        	}
        	if (checkResults.isEmpty()) {
        		throw new IllegalArgumentException("Check ID not found: " + checkId);
        	}
        	
        	categorizedResults.put(fairCategory, checkResults);
        	columnCheckResults.put(columnName, categorizedResults);
        }
		return columnCheckResults;
	}
	
	public Map<String, Map<String, List<FairCheckResult>>> executeSpecificCheckInSpecificColumn(DatasetProfile profile, String column, String checkId) {
		Dataset<Row> dataset = profile.getDataset();
        String[] columnNames = dataset.columns();
        Map<String, Map<String, List<FairCheckResult>>> columnCheckResults = new HashMap<>();
        
        String fairCategory = getFairCategory(checkId);
        
        List<FairCheckResult> checkResults = new ArrayList<>();
        
        for (String columnName : columnNames) {
        	if (columnName.equalsIgnoreCase(column)) {
        		Map<String, List<FairCheckResult>> categorizedResults = new HashMap<>();
            	
            	List<IGenericCheck> columnChecks = ColumnFairCheckFactory.getAllColumnChecks(columnName);

            	for (IGenericCheck check : columnChecks) {
            		if (check.getCheckId() == checkId) {
            			boolean result = check.executeCheck(dataset);
    	        	    List<String> invalidRows = ((IGenericColumnCheck) check).getInvalidRows();
            			checkResults.add(new FairCheckResult(check.getCheckId(), check.getCheckDescription(), result, invalidRows));
            			break;
            		}
            	}
            	if (checkResults.isEmpty()) {
            		throw new IllegalArgumentException("Check ID not found: " + checkId);
            	}
            	
            	categorizedResults.put(fairCategory, checkResults);
            	columnCheckResults.put(columnName, categorizedResults);
            	break;
        	}
        }
        if (columnCheckResults.isEmpty()) {
        	throw new IllegalArgumentException("Column name not found: " + column);
        }
		return columnCheckResults;
	}
	
	public Map<String, Map<String, List<FairCheckResult>>> executeChecksInSpecificColumn(DatasetProfile profile, String column) {
		Dataset<Row> dataset = profile.getDataset();
        String[] columnNames = dataset.columns();
        Map<String, Map<String, List<FairCheckResult>>> columnCheckResults = new HashMap<>();
        
        for (String columnName : columnNames) {
        	if (columnName.equalsIgnoreCase(column)) {
        		columnCheckResults = executeAllColumnFairChecks(dataset, columnName, columnCheckResults);
        		break;
        	}
        }
        if (columnCheckResults.isEmpty()) {
        	throw new IllegalArgumentException("Column name not found: " + column);
        }
		return columnCheckResults;
	}
	
	private String getFairCategory(String checkId) {
		if (checkId.startsWith("F")) {
			return "Findability";
		}
		else if (checkId.startsWith("A")) {
			return "Accessibility";
		}
		else if (checkId.startsWith("I")) {
			return "Interoperability";
		}
		else if (checkId.startsWith("R")) {
			return "Reusability";
		}
		return null;
	}
	
	private Map<String, Map<String, List<FairCheckResult>>> executeAllColumnFairChecks(Dataset<Row> dataset, String columnName, 
			Map<String, Map<String, List<FairCheckResult>>> columnCheckResults){
		
		DataType columnType = dataset.schema().apply(columnName).dataType();
    	Map<String, List<FairCheckResult>> categorizedResults = new HashMap<>();
    	
    	// Execute Findability Checks
    	List<IGenericCheck> findabilityChecks = ColumnFairCheckFactory.getFindabilityChecks(columnName);
    	List<FairCheckResult> findabilityResults = new ArrayList<>();
    	for (IGenericCheck check : findabilityChecks) {
    		if (((IGenericColumnCheck) check).isApplicable(columnType)) {
        	    boolean result = check.executeCheck(dataset);
        	    List<String> invalidRows = ((IGenericColumnCheck) check).getInvalidRows();
        	    findabilityResults.add(new FairCheckResult(check.getCheckId(), check.getCheckDescription(), result, invalidRows));
    		}
    	}
    	categorizedResults.put("Findability", findabilityResults);

        // Execute Accessibility Checks
    	List<IGenericCheck> accessibilityChecks = ColumnFairCheckFactory.getAccessibilityChecks(columnName);
    	List<FairCheckResult> accessibilityResults = new ArrayList<>();
    	for (IGenericCheck check : accessibilityChecks) {
    		if (((IGenericColumnCheck) check).isApplicable(columnType)) {
        	    boolean result = check.executeCheck(dataset);
        	    List<String> invalidRows = ((IGenericColumnCheck) check).getInvalidRows();
        	    accessibilityResults.add(new FairCheckResult(check.getCheckId(), check.getCheckDescription(), result, invalidRows));
        	}
    	}
    	categorizedResults.put("Accessibility", accessibilityResults);

        // Execute Interoperability Checks
    	List<IGenericCheck> interoperabilityChecks = ColumnFairCheckFactory.getInteroperabilityChecks(columnName);
    	List<FairCheckResult> interoperabilityResults = new ArrayList<>();
    	for (IGenericCheck check : interoperabilityChecks) {
    		if (((IGenericColumnCheck) check).isApplicable(columnType)) {
        	    boolean result = check.executeCheck(dataset);
        	    List<String> invalidRows = ((IGenericColumnCheck) check).getInvalidRows();
        	    interoperabilityResults.add(new FairCheckResult(check.getCheckId(), check.getCheckDescription(), result, invalidRows));
        	}
    	}
    	categorizedResults.put("Interoperability", interoperabilityResults);

        // Execute Reusability Checks
    	List<IGenericCheck> reusabilityChecks = ColumnFairCheckFactory.getReusabilityChecks(columnName);
    	List<FairCheckResult> reusabilityResults = new ArrayList<>();
    	for (IGenericCheck check : reusabilityChecks) {
    		if (((IGenericColumnCheck) check).isApplicable(columnType)) {
        	    boolean result = check.executeCheck(dataset);
        	    List<String> invalidRows = ((IGenericColumnCheck) check).getInvalidRows();
        	    reusabilityResults.add(new FairCheckResult(check.getCheckId(), check.getCheckDescription(), result, invalidRows));
        	}
    	}
    	categorizedResults.put("Reusability", reusabilityResults);

        columnCheckResults.put(columnName, categorizedResults);
		return columnCheckResults;
	}
}
