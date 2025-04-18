package model;

import java.util.List;

/**
 * Represents the result of a single FAIR data quality check executed on a dataset column or the entire dataset.
 * 
 *@param checkId A string, unique identifier of the check.
 *@param description Human-readable explanation of what the check validates
 *@param passed True if the dataset passed the check, false otherwise
 *@param invalidRows List of row-level errors or violations (empty if check passed)
 */
public class FairCheckResult {
	
	private String checkId;
    private String description;
    private boolean passed;
    private List<String> invalidRows;

    public FairCheckResult(String checkId, String description, boolean passed, List<String> invalidRows) {
        this.checkId = checkId;
        this.description = description;
        this.passed = passed;
        this.invalidRows = invalidRows;
    }

    public String getCheckId() { return checkId; }
    public String getDescription() { return description; }
    public boolean isPassed() { return passed; }
    public List<String> getInvalidRows() { return invalidRows; }

}
