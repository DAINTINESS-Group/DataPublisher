package engine;

import java.util.List;
import java.util.Map;

import model.DatasetProfile;
import model.FairCheckResult;
import utils.RegistrationResponse;
import utils.ReportType;

/**
 * Interface representing the main gateway for FAIR-compliant dataset validation and processing.
 * 
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
	 * Registers a dataset by its path and alias, indicating if it contains a header row.
	 * 
	 * @param path the file system path to the dataset
	 * @param alias a unique alias to identify the dataset internally
	 * @param hasHeader whether the dataset includes a header row
	 * @return a {@link RegistrationResponse} containing success or error metadata
	 */
	public RegistrationResponse registerDataset(String path, String alias, boolean hasHeader);
	
	/**
	 * 
	 * Executes all global FAIR checks for the specified dataset.
	 * 
	 * @param datasetAlias the alias of the dataset on which to execute the checks
	 * @return a map of check IDs to boolean results
	 * @throws IllegalStateException if the dataset is not registered or accessible
	 */
	public Map<String, Boolean> executeGlobalChecks(String datasetAlias) throws IllegalStateException;
	
	/**
	 * 
	 * Executes a specific global check, for the specified dataset.
	 * 
	 * @param datasetAlias the alias of the dataset on which to execute the checks
	 * @param checkId the ID of the check to execute
	 * @return a map of the check ID and its boolean result
	 * @throws IllegalStateException if the dataset is not registered or accessible
	 */
	public Map<String, Boolean> executeGlobalChecks(String datasetAlias, String checkId) throws IllegalStateException;
	
	/**
	 * 
	 * Executes all applicable column-level FAIR checks for the specified dataset.
	 * 
	 * @param datasetAlias the alias of the dataset on which to execute the checks
	 * @return a nested map: column name, FAIR category, check ID, list of results
	 * @throws IllegalStateException if the dataset is not registered or accessible
	 */
	public Map<String, Map<String, List<FairCheckResult>>> executeColumnChecks(String datasetAlias) throws IllegalStateException;
	
	/**
	 * 
	 * Executes column-level FAIR checks for the specified dataset. 
	 * 
	 * <p>This method supports two modes of operation based on the input parameters:
	 * <ul>
	 *   <li><strong>Single-column mode:</strong> If {@code checkId} is {@code "all"}, all applicable checks will be executed on the specified {@code column}.</li>
	 *   <li><strong>Single-check mode:</strong> If {@code column} is {@code "all"}, the specified {@code checkId} will be executed across all applicable columns.</li>
	 * </ul>
	 * 
	 * @param datasetAlias the alias of the dataset on which to execute the checks
	 * @param column the name of the column to check (or {@code "all"} to apply the check to all columns)
	 * @param checkId the ID of the check to execute (or {@code "all"} to apply all checks to the specified column)
	 * @return a nested map: column name, FAIR category, check ID, list of results
	 * @throws IllegalStateException if the dataset is not registered or cannot be accessed
	 */
	public Map<String, Map<String, List<FairCheckResult>>> executeColumnChecks(String datasetAlias, String column, String checkId) throws IllegalStateException;
	
	/**
	 * 
	 * Generates a report summarizing the results of global checks.
	 * 
	 * @param datasetAlias the alias of the dataset
	 * @param globalResults the results of global checks
	 * @param outputPath the file path to write the report to
	 * @param reportType the type of the report file
	 */
	public void generateGlobalReport(String datasetAlias, Map<String, Boolean> globalResults, String outputPath, ReportType reportType);
	
	/**
	 * 
	 * Generates a report summarizing the results of column-level checks.
	 * 
	 * @param datasetAlias the alias of the dataset
	 * @param columnResults the results of column checks
	 * @param outputPath the file path to write the report to
	 * @param reportType the type of the report file
	 */
	public void generateColumnReport(String datasetAlias, Map<String, Map<String, List<FairCheckResult>>> columnResults, String outputPath, ReportType reportType);

	/**
	 * 
	 * Retrieves a profile object containing summary metadata for the specified dataset.
	 * 
	 * @param alias the alias of the dataset
	 * @return a {@link DatasetProfile} object
	 */
	public DatasetProfile getProfile(String alias);
}
