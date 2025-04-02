package fairchecks.checks;

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
    	// 1. Check metadata flag
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

        // 2. Check file-level access
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

            // Optional: Try reading a few bytes to confirm access
            try (FileInputStream fis = new FileInputStream(file)) {
                fis.read();
            }

        } catch (Exception e) {
            System.out.println("Access restriction detected while opening file: " + e.getMessage());
            return false;
        }

        return true; // If all checks pass, file is accessible
    }

}
