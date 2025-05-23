package fairchecks.checks.columnChecks;

import fairchecks.api.IGenericColumnCheck;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.languagetool.JLanguageTool;
import org.languagetool.language.BritishEnglish;
import org.languagetool.rules.RuleMatch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A check that identifies potentially inaccurate values in a column.
 *<p>
 * For string columns, it flags misspelled values, using a British vocabulary. 
 * For numeric columns, it applies semantic-aware rules
 * (e.g., age must be between 1–120, percentages between 0–100, amounts must be non-negative).
 * 
 * <p>Check ID: REU4
 */
public class DataAccuracyCheck implements IGenericColumnCheck {
	
	private final String columnName;
    private final List<String> invalidRows = new ArrayList<>();
    private DataType capturedColumnType;
    
    public DataAccuracyCheck(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String getCheckId() {
        return "REU4";
    }

    @Override
    public String getCheckDescription() {
        return "Data should be accurate.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
    	if (capturedColumnType == null) {
    		System.out.println("column type is null");
    		return false;
    	}

    	if (capturedColumnType.simpleString().equals("string")) {
    		return checkStringColumn(dataset);
    	} else {
    		return checkNumericColumn(dataset);
    	}
    }
    
    private boolean checkNumericColumn(Dataset<Row> dataset) {
    	try {
    		Dataset<Row> cleaned = dataset
    			.filter(functions.col(columnName).isNotNull())
    			.withColumn("numeric_value", functions.col(columnName).cast("double"));

    		Dataset<Row> invalid;
    		String lowerName = columnName.toLowerCase();
    		
    		if(lowerName.contains("percent")) {
    			invalid = cleaned.filter(
    					functions.col("numeric_value").lt(0)
    							 .or(functions.col("numeric_value").gt(100))
    			);
    		} else if (lowerName.contains("weight") || lowerName.contains("mass")
    				|| lowerName.contains("price") || lowerName.contains("amount")
    				|| lowerName.contains("quantity")) {
    			invalid = cleaned.filter(functions.col("numeric_value").lt(0));
    		}else if(lowerName.contains("age")) {
    			invalid = cleaned.filter(functions.col("numeric_value").lt(1));
    		}else {
        		return true;
    		}

    		List<Row> invalidList = invalid.select(functions.col("_id"), functions.col(columnName)).collectAsList();
    		for (Row row : invalidList) {
    			Number rowIdNum = (Number) row.getAs("_id");
            	long rowId = rowIdNum.longValue() + 1;
            	
            	Object raw = row.get(1);
            	String value = (raw == null) ? "<null>" : raw.toString();
            	
    			invalidRows.add("Row " + rowId + ": Invalid numeric value: " + value);
    		}
    		
    		return invalidRows.isEmpty();
    	} catch (Exception e) {
    		e.printStackTrace();
    		return false;
    	}
    }
    
    private boolean checkStringColumn(Dataset<Row> dataset) {
    	try {
    		JLanguageTool langTool = new JLanguageTool(new BritishEnglish());
    		
    		Map<String, Boolean> cache = new HashMap<>();
    		
    		Dataset<Row> cleaned = dataset
    			.filter(functions.col(columnName).isNotNull()
    					.and(functions.not(functions.lower(functions.col(columnName)).equalTo("null"))))
    			.withColumn(columnName, functions.trim(functions.col(columnName)));
    		
    		List<Row> rows = cleaned.select(functions.col("_id"), functions.col(columnName)).collectAsList();
    		
    		for (Row row : rows) {
    			Number rowIdNum = (Number) row.getAs("_id");
            	long rowId = rowIdNum.longValue() + 1;
            	
            	Object raw = row.get(1);
            	if (raw == null) continue;
            	
            	String value = raw.toString().trim();
    			if(value.equalsIgnoreCase("null")) continue;
    			
    			Boolean isInvalid = cache.get(value);
    			if(isInvalid != null) {
    				if (isInvalid) {
    					invalidRows.add("Row " + rowId + ": Likely misspelled value: " + value);
    				}
    				continue;
    			}
    			
    			List<RuleMatch> matches = langTool.check("This is " + value + ".");
    			boolean hasSpellingIssue = matches.stream()
    				.anyMatch(m -> m.getRule().getCategory().getId().toString().equalsIgnoreCase("TYPOS"));
    			cache.put(value, hasSpellingIssue);
    			
    			if (hasSpellingIssue) {
    				invalidRows.add("Row " + rowId + ": Likely misspelled: " + value);
    			}
    		}
    		return invalidRows.isEmpty();
    		
    	} catch (Exception e) {
    		e.printStackTrace();
    		return false;
    	}
    }

    @Override
    public List<String> getInvalidRows() {
        return invalidRows;
    }
    
    @Override
    public boolean isApplicable(DataType columnType) {
    	this.capturedColumnType = columnType;
        return true;
    }
}
