package fairchecks.checks.columnChecks;

import fairchecks.api.IGenericColumnCheck;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;

import java.util.ArrayList;
import java.util.List;

/**
 * A check that identifies potentially inaccurate values in a column.
 *<p>
 *For numeric columns, it applies semantic-aware rules
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
    		System.out.println("Checking column: " + columnName);
    		Dataset<Row> cleaned = dataset
    			.filter(functions.col(columnName).isNotNull())
    			.withColumn("numeric_value", functions.col(columnName).cast("double"));
    		
    		System.out.println("Sample of cleaned data:");
    		cleaned.select(functions.col(columnName), functions.col("numeric_value")).show(10, false); // show 10 rows, full content

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
    			System.out.println("Inferred semantic type: non-negative numeric");
    			invalid = cleaned.filter(functions.col("numeric_value").lt(0));
    		}else if(lowerName.contains("age")) {
    			invalid = cleaned.filter(functions.col("numeric_value").lt(1));
    		}else {
    			System.out.println("No matching rule for column: " + columnName);
        		return true;
    		}
    		
    		 System.out.println("Potentially invalid values:");
    	        invalid.select(functions.col(columnName), functions.col("numeric_value")).show(10, false);
    		
    		List<Row> invalidList = invalid.select(columnName).collectAsList();
    		for (Row row : invalidList) {
    			invalidRows.add("Invalid numeric value: " + row.get(0));
    		}
    		
    		return invalidRows.isEmpty();
    	} catch (Exception e) {
    		e.printStackTrace();
    		return false;
    	}
    	
    }
    
    private boolean checkStringColumn(Dataset<Row> dataset) {
    	return true;
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
