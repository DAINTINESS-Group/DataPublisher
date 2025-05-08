package report;

import java.util.List;
import java.util.Map;
import model.FairCheckResult;

public interface IChecksReportGenerator {
	
	void generateGlobalReport(String alias, Map<String, Boolean> results, String outputPath);
    void generateColumnReport(String alias, Map<String, Map<String, List<FairCheckResult>>> results, String outputPath);

}
