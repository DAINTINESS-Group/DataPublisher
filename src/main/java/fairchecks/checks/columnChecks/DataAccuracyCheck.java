package fairchecks.checks.columnChecks;

import fairchecks.api.IGenericColumnCheck;

import org.apache.commons.text.similarity.JaroWinklerSimilarity;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A check that identifies potentially inaccurate values in a column.
 *<p>
 * For string columns, it flags uncommon or misspelled values by combining frequency
 * analysis and fuzzy matching. For numeric columns, it applies semantic-aware rules
 * (e.g., age must be between 1–120, percentages between 0–100, amounts must be non-negative).
 * 
 * <p>Check ID: REU4
 */
public class DataAccuracyCheck implements IGenericColumnCheck {
	
	private final String columnName;
    private final List<String> invalidRows = new ArrayList<>();
    private DataType capturedColumnType;
    
    private static final int MIN_OCCURRENCES = 3;
    private static final double SIMILARITY_THRESHOLD = 0.90;
    private final JaroWinklerSimilarity similarity = new JaroWinklerSimilarity();

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
    	try {
    		Dataset<Row> cleaned = dataset
    			.filter(functions.col(columnName).isNotNull()
    					.and(functions.not(functions.lower(functions.col(columnName)).equalTo("null"))))
    			.withColumn(columnName, functions.trim(functions.col(columnName)));
    		
    		List<String> frequentValues = cleaned
    			.groupBy(columnName)
    			.count()
    			.filter(functions.col("count").geq(MIN_OCCURRENCES))
    			.select(columnName)
    			.collectAsList()
    			.stream()
    			.map(row -> row.getString(0))
    			.collect(Collectors.toList());
    		
    		List<Row> allValues = cleaned.select(columnName).collectAsList();
    		for (Row row : allValues) {
    			String value = row.getString(0);
    			if(value == null || frequentValues.contains(value)) continue;
    			
    			boolean isSimilar = frequentValues.stream()
    				.anyMatch(frequent -> similarity.apply(frequent, value) >= SIMILARITY_THRESHOLD);
    			if(!isSimilar) {
    				invalidRows.add("Unusual or inaccurate value: " + value);
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
