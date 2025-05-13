package engine;

import java.util.List;
import java.util.Map;

import model.DatasetProfile;
import model.FairCheckResult;
import utils.RegistrationResponse;
import utils.ReportType;

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
	 * @return a {@link RegistrationResponse} containing success or error metadata
	 */
	public RegistrationResponse registerDataset(String path, String alias, boolean hasHeader);
	
	/**
	 * 
	 * @param datasetAlias
	 * @return a map of check IDs to boolean results
	 * @throws IllegalStateException if the dataset is not registered or accessible
	 */
	public Map<String, Boolean> executeGlobalChecks(String datasetAlias) throws IllegalStateException;
	
	/**
	 * 
	 * @param datasetAlias
	 * @param checkId
	 * @return
	 * @throws IllegalStateException
	 */
	public Map<String, Boolean> executeGlobalChecks(String datasetAlias, String checkId) throws IllegalStateException;
	
	/**
	 * 
	 * @param datasetAlias
	 * @return a nested map: column name, check ID, list of results
	 * @throws IllegalStateException if the dataset is not registered or accessible
	 */
	public Map<String, Map<String, List<FairCheckResult>>> executeColumnChecks(String datasetAlias) throws IllegalStateException;
	
	/**
	 * 
	 * @param datasetAlias
	 * @param column
	 * @param checkId
	 * @return
	 * @throws IllegalStateException
	 */
	public Map<String, Map<String, List<FairCheckResult>>> executeColumnChecks(String datasetAlias, String column, String checkId) throws IllegalStateException;
	
	/**
	 * 
	 * @param datasetAlias
	 * @param globalResults
	 * @param outputPath
	 * @param reportType
	 */
	public void generateGlobalReport(String datasetAlias, Map<String, Boolean> globalResults, String outputPath, ReportType reportType);
	
	/**
	 * 
	 * @param datasetAlias
	 * @param columnResults
	 * @param outputPath
	 * @param reportType
	 */
	public void generateColumnReport(String datasetAlias, Map<String, Map<String, List<FairCheckResult>>> columnResults, String outputPath, ReportType reportType);

	/**
	 * 
	 * @param alias
	 * @return a {@link DatasetProfile} object
	 */
	public DatasetProfile getProfile(String alias);
}
