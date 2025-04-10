package model;

import java.util.List;

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
