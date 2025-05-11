package report;

import java.util.List;
import java.util.Map;
import model.FairCheckResult;

/**
 * Interface for generating reports that summarize the results of FAIR data checks.
 *
 * <p>Implementations of this interface are responsible for converting the results of global and
 * column-level checks into human-readable reports.
 *
 */
public interface IChecksReportGenerator {
	
	/**
	 * 
	 * @param alias
	 * @param results
	 * @param outputPath
	 */
	public void generateGlobalReport(String alias, Map<String, Boolean> results, String outputPath);
    
	/**
	 * 
	 * @param alias
	 * @param results
	 * @param outputPath
	 */
	public void generateColumnReport(String alias, Map<String, Map<String, List<FairCheckResult>>> results, String outputPath);

}