package fairchecks.checks.globalChecks;

import fairchecks.api.IInteroperabilityCheck;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

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
