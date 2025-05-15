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
	 * @param alias the alias of the dataset
	 * @param results the results of global checks
	 * @param outputPath the file path to write the report to
	 */
	public void generateGlobalReport(String alias, Map<String, Boolean> results, String outputPath);
    
	/**
	 * 
	 * @param alias the alias of the dataset
	 * @param results the results of column checks
	 * @param outputPath the file path to write the report to
	 */
	public void generateColumnReport(String alias, Map<String, Map<String, List<FairCheckResult>>> results, String outputPath);

}