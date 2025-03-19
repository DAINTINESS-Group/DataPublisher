package fairchecks.checks;

import fairchecks.api.IInteroperabilityCheck;

import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class XmlToolIndependenceCheck implements IInteroperabilityCheck {
	
	@Override
    public String getCheckId() {
        return "IEU19";
    }

    @Override
    public String getCheckDescription() {
        return "XML - Files should not depend on specific programs or tools used to process them.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
        try {
            String filePath = dataset.sparkSession().conf().get("spark.sql.xml.filepath", "").toLowerCase();
            
            String delimiter = dataset.sparkSession().conf().get("spark.sql.csv.delimiter", ";");
            org.apache.spark.sql.Column[] columns = Arrays.stream(dataset.columns())
                    .map(functions::col)
                    .toArray(org.apache.spark.sql.Column[]::new);

            if (!filePath.endsWith(".xml")) {
                return true;
            }

            long toolSpecificTagCount = dataset.filter(
                functions.lower(functions.concat_ws(delimiter, columns))
                .rlike(".*(mso|adobe|autocad|office|generatedBy).*")
            ).count();

            return toolSpecificTagCount == 0;
        } catch (Exception e) {
            System.err.println("Error executing XML Tool Independence Check: " + e.getMessage());
            return false;
        }
    }

}
