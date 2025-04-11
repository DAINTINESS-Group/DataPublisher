package fairchecks.checks.globalChecks;

import fairchecks.api.IAccessibilityCheck;

import java.io.File;
import java.io.FileInputStream;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataAccessRestrictionCheck implements IAccessibilityCheck {
	
	@Override
    public String getCheckId() {
        return "AEU1";
    }

    @Override
    public String getCheckDescription() {
        return "Data to be published without access restrictions.";
    }

    @Override
    public boolean executeCheck(Dataset<Row> dataset) {
        try {
            String declaredRestricted = dataset.sparkSession()
                    .conf()
                    .get("data.access.restricted", "false");

            if (declaredRestricted.equalsIgnoreCase("true")) {
                System.out.println("Declared restriction found in config.");
                return false;
            }
        } catch (Exception e) {
            System.err.println("Error reading config: " + e.getMessage());
        }

        try {
            String filePath = dataset.sparkSession().conf().get("spark.sql.csv.filepath", null);
            if (filePath == null) {
                System.out.println("No file path provided for access check.");
                return true;
            }

            File file = new File(filePath);
            if (!file.exists() || !file.canRead()) {
                System.out.println("File is either missing or not readable by this process.");
                return false;
            }

            try (FileInputStream fis = new FileInputStream(file)) {
                fis.read();
            }

        } catch (Exception e) {
            System.out.println("Access restriction detected while opening file: " + e.getMessage());
            return false;
        }

        return true;
    }

}
