package fairchecks.factory;

import fairchecks.api.*;
import fairchecks.checks.columnChecks.ControlledVocabularyCheck;
import fairchecks.checks.columnChecks.CsvUnitInHeaderCheck;
import fairchecks.checks.columnChecks.DataAccuracyCheck;
import fairchecks.checks.columnChecks.DateTimeFormatCheck;
import fairchecks.checks.columnChecks.DecimalFormatCheck;
import fairchecks.checks.columnChecks.NullValueMarkingCheck;
import fairchecks.checks.columnChecks.SpecialCharacterCheck;
import fairchecks.checks.columnChecks.ThousandsSeparatorCheck;

//import fairchecks.checks.columnChecks.*;
import java.util.ArrayList;
import java.util.List;

public class ColumnFairCheckFactory {
	
	public static List<IFindabilityCheck> getFindabilityChecks(String columnName){
		List<IFindabilityCheck> columnChecks = new ArrayList<>();
		columnChecks.add(new NullValueMarkingCheck(columnName)); // FEU2
		
		return columnChecks;
	}
	
	public static List<IAccessibilityCheck> getAccessibilityChecks(String columnName){
		List<IAccessibilityCheck> columnChecks = new ArrayList<>();
		
		return columnChecks;
	}
	
	public static List<IInteroperabilityCheck> getInteroperabilityChecks(String columnName){
		List<IInteroperabilityCheck> columnChecks = new ArrayList<>();
		columnChecks.add(new DateTimeFormatCheck(columnName));   // IEU1
        columnChecks.add(new DecimalFormatCheck(columnName));    // IEU2.1
        columnChecks.add(new ThousandsSeparatorCheck(columnName)); // IEU2.2
        columnChecks.add(new SpecialCharacterCheck(columnName)); // IEU3.2
        columnChecks.add(new ControlledVocabularyCheck(columnName));   // IEU4
        columnChecks.add(new CsvUnitInHeaderCheck(columnName));     // IEU17.1
		
		return columnChecks;
	}
	
	public static List<IReusabilityCheck> getReusabilityChecks(String columnName){
		List<IReusabilityCheck> columnChecks = new ArrayList<>();
		columnChecks.add(new DataAccuracyCheck(columnName)); // REU4
		
		return columnChecks;
	}

}
