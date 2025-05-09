package fairchecks.checks.globalChecks;

import fairchecks.api.IGenericCheck;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * A global check that ensures measurement units are not embedded within data values
 * when units vary and are instead represented in a dedicated column.
 *
 * <p>Check ID: IEU17.3
 */
public class CsvUnitInDedicatedColumnCheck implements IGenericCheck {
	
    @Override
    public String getCheckId() {
        return "IEU17.3";
    }

    @Override
    public String getCheckDescription() {
        return "If the unit varies within a column, the dataset shall have a special column for it.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
    	for (StructField field : dataset.schema().fields()) {
            String columnName = field.name();

            if (field.dataType() instanceof StringType) {
                Dataset<Row> nonNullValues = dataset.select(columnName)
                                                    .filter(functions.col(columnName).isNotNull())
                                                    .limit(100);

                boolean allNumeric = true;
                boolean containsNumberWithUnit = false;

                for (Row row : nonNullValues.collectAsList()) {
                    String value = row.getString(0);
                    if (value != null) {
                        if (isNumeric(value)) {
                            continue;
                        } else if (containsNumberWithUnit(value)) {
                            containsNumberWithUnit = true;
                        } else {
                            allNumeric = false;
                            break;
                        }
                    }
                }

                boolean hasParentheses = columnName.matches(".*\\(.*\\).*");

                if ((allNumeric || containsNumberWithUnit) && !hasParentheses) {
                    return false;
                }
            }
        }
    	return true;
    }
    
    private boolean isNumeric(String str) {
        if (str == null) {
            return false;
        }
        try {
            Double.parseDouble(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    
    private boolean containsNumberWithUnit(String str) {
        if (str == null) {
            return false;
        }
        Pattern pattern = Pattern.compile("^\\d+(\\.\\d+)?\\s*[a-zA-Z]+$");
        Matcher matcher = pattern.matcher(str);
        return matcher.matches();
    }
}
