package fairchecks.api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A generic interface for all FAIR data checks, defining the core contract
 * that each specific check implementation must follow.
 *
 */
public interface IGenericCheck {
	
	/**
     * Returns the unique identifier for the check.
     *
     * @return the check ID string (e.g., "IEU4", "REU1")
     */
	public String getCheckId();
	
	/**
     * Returns a human-readable description of what the check validates.
     *
     * @return a brief explanation of the check’s purpose
     */
	public String getCheckDescription();
	
	/**
     * Executes the check on the provided dataset.
     *
     * @param dataset
     * @return {@code true} if the dataset passes the check, {@code false} otherwise
     */
	public boolean executeCheck(Dataset<Row> dataset);
}