package fairchecks.factory;

import fairchecks.api.*;
import fairchecks.checks.*;

import java.util.ArrayList;
import java.util.List;

public class GlobalFairCheckFactory {
	
	public static List<IFindabilityCheck> getFindabilityChecks(){
		List<IFindabilityCheck> checks = new ArrayList<>();
		
		return checks;
	}
	
	public static List<IAccessibilityCheck> getAccessibilityChecks(){
		List<IAccessibilityCheck> checks = new ArrayList<>();
		checks.add(new DataAccessRestrictionCheck());  // AEU1
		
		return checks;
	}
	
	public static List<IInteroperabilityCheck> getInteroperabilityChecks(){
		List<IInteroperabilityCheck> checks = new ArrayList<>();
	    checks.add(new Utf8EncodingCheck());  // IEU3.1
	    checks.add(new UriDetailsRetrievalCheck());  // IEU6
	    checks.add(new UriLinkedDataCheck());  // IEU7
	    checks.add(new CsvSemicolonSeparatorCheck());  // IEU9
	    checks.add(new CsvSingleTableCheck());  // IEU10
	    checks.add(new CsvNoAdditionalInfoCheck());  // IEU11.1
	    checks.add(new CsvTitleInDistributionCheck());  // IEU11.2
	    checks.add(new CsvSingleHeaderCheck());  // IEU11.3
	    checks.add(new CsvSingleSheetCheck());  // IEU11.4
	    checks.add(new CsvColumnHeaderCheck());  // IEU15
	    checks.add(new CsvUniformColumnCountCheck());  // IEU16
	    checks.add(new CsvUnitInDedicatedColumnCheck()); // IEU17.3
	    
		return checks;
	}
	
	public static List<IReusabilityCheck> getReusabilityChecks(){
		List<IReusabilityCheck> checks = new ArrayList<>();
		checks.add(new SufficientDataCheck());  // REU1
		checks.add(new DuplicateDataCheck()); // REU3
	    checks.add(new SeparateSheetDatasetCheck());  // REU7.5
	    
		return checks;
	}
}
