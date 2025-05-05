package engine;

import java.util.List;
import java.util.Map;

import model.DatasetProfile;
import model.FairCheckResult;
import utils.RegistrationResponse;

/**
 * Interface representing the main gateway for FAIR-compliant dataset validation and processing.
 * Classes implementing <code>IDataPublisherFacade</code> handle the registration of datasets, execution of 
 * FAIR data quality checks (both global and column-based), generation of a report with the results of the checks and access to dataset profiles.
 * 
 * This interface acts as the primary abstraction layer between external clients and the internal components 
 * of the data quality checking engine, following the Facade design pattern.
 *
 * @see model.DatasetProfile
 * @see model.FairCheckResult
 */
public interface IDataPublisherFacade {
	
	/**
	 * 
	 * @param path
	 * @param alias
	 * @param hasHeader
	 * @return
	 */
	public RegistrationResponse registerDataset(String path, String alias, boolean hasHeader);
	
	/**
	 * 
	 * @param datasetAlias
	 * @return
	 */
	public Map<String, Boolean> executeGlobalChecks(String datasetAlias) throws IllegalStateException;
	
	/**
	 * 
	 * @param datasetAlias
	 * @return
	 */
	public Map<String, Map<String, List<FairCheckResult>>> executeColumnChecks(String datasetAlias) throws IllegalStateException;
	
	/**
	 * 
	 * @param datasetAlias
	 * @param globalResults
	 * @param outputPath
	 */
	public void generateGlobalReport(String datasetAlias, Map<String, Boolean> globalResults, String outputPath);
	
	/**
	 * 
	 * @param datasetAlias
	 * @param columnResults
	 * @param outputPath
	 */
	public void generateColumnReport(String datasetAlias, Map<String, Map<String, List<FairCheckResult>>> columnResults, String outputPath);

	/**
	 * 
	 * @param alias
	 * @return
	 */
	public DatasetProfile getProfile(String alias);
}
