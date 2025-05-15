package fairchecks.factory;

import fairchecks.api.IGenericCheck;
import fairchecks.checks.columnChecks.ControlledVocabularyCheck;
import fairchecks.checks.columnChecks.CsvUnitInHeaderCheck;
import fairchecks.checks.columnChecks.DataAccuracyCheck;
import fairchecks.checks.columnChecks.DateTimeFormatCheck;
import fairchecks.checks.columnChecks.DecimalFormatCheck;
import fairchecks.checks.columnChecks.NullValueMarkingCheck;
import fairchecks.checks.columnChecks.SpecialCharacterCheck;
import fairchecks.checks.columnChecks.ThousandsSeparatorCheck;

import java.util.ArrayList;
import java.util.List;

/**
 * Factory class responsible for creating instances of column FAIR checks, organized all together or by FAIR principle.
 * <p>
 * Each method returns a list of check implementations related to a specific principle:
 * Findability, Accessibility, Interoperability, or Reusability, or all column checks.
 * </p>
 * 
 * @see fairchecks.checks.columnChecks
 */
public class ColumnFairCheckFactory {
	
	public static List<IGenericCheck> getAllColumnChecks(String columnName) {
		List<IGenericCheck> columnChecks = new ArrayList<>();
		columnChecks.add(new NullValueMarkingCheck(columnName)); // FEU2
		columnChecks.add(new DateTimeFormatCheck(columnName));   // IEU1
        columnChecks.add(new DecimalFormatCheck(columnName));    // IEU2.1
        columnChecks.add(new ThousandsSeparatorCheck(columnName)); // IEU2.2
        columnChecks.add(new SpecialCharacterCheck(columnName)); // IEU3.2
        columnChecks.add(new ControlledVocabularyCheck(columnName));   // IEU4
        columnChecks.add(new CsvUnitInHeaderCheck(columnName));     // IEU17.1
        columnChecks.add(new DataAccuracyCheck(columnName)); // REU4
        
		return columnChecks;
	}
	
	public static List<IGenericCheck> getFindabilityChecks(String columnName){
		List<IGenericCheck> columnChecks = new ArrayList<>();
		columnChecks.add(new NullValueMarkingCheck(columnName)); // FEU2
		
		return columnChecks;
	}
	
	public static List<IGenericCheck> getAccessibilityChecks(String columnName){
		List<IGenericCheck> columnChecks = new ArrayList<>();
		
		return columnChecks;
	}
	
	public static List<IGenericCheck> getInteroperabilityChecks(String columnName){
		List<IGenericCheck> columnChecks = new ArrayList<>();
		columnChecks.add(new DateTimeFormatCheck(columnName));   // IEU1
        columnChecks.add(new DecimalFormatCheck(columnName));    // IEU2.1
        columnChecks.add(new ThousandsSeparatorCheck(columnName)); // IEU2.2
        columnChecks.add(new SpecialCharacterCheck(columnName)); // IEU3.2
        columnChecks.add(new ControlledVocabularyCheck(columnName));   // IEU4
        columnChecks.add(new CsvUnitInHeaderCheck(columnName));     // IEU17.1
		
		return columnChecks;
	}
	
	public static List<IGenericCheck> getReusabilityChecks(String columnName){
		List<IGenericCheck> columnChecks = new ArrayList<>();
		columnChecks.add(new DataAccuracyCheck(columnName)); // REU4
		
		return columnChecks;
	}

}
