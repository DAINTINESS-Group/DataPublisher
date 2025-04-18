package fairchecks.checks.globalChecks;

import fairchecks.api.IInteroperabilityCheck;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A global check that verifies whether a CSV file includes human-readable
 * column headers in its first row, rather than default auto-generated names.
 * <p>The check considers headers missing if all column names follow the Spark default
 * pattern {@code _c0, _c1, _c2, ...}, which typically indicates that no header was provided.
 * 
 * <p>Check ID: IEU15
 */
public class CsvColumnHeaderCheck implements IInteroperabilityCheck{
	
	@Override
    public String getCheckId() {
        return "IEU15";
    }

    @Override
    public String getCheckDescription() {
        return "CSV - Data files should contain column headers as their first row.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
    	try {
    		String[] allColumnNames = dataset.columns();
            Set<String> columnsToExclude = new HashSet<>(Arrays.asList("_id"));

            List<String> relevantColumnNames = Arrays.stream(allColumnNames)
                                                     .filter(name -> !columnsToExclude.contains(name))
                                                     .collect(Collectors.toList());

            boolean hasDefaultColumnNames = relevantColumnNames.stream()
                    .allMatch(name -> name.matches("_c\\d+"));
			
			return !hasDefaultColumnNames;
        } catch (Exception e) {
            System.err.println("Error executing CSV Column Header Check: " + e.getMessage());
            return false;
        }
    }
}
