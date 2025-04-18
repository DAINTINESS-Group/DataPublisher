package report;

import model.FairCheckResult;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Class for generating human-readable FAIR check reports in plain text format.
 * <p>
 * This class provides methods to export the results of global and column-based FAIR checks
 * to a specified output file. These reports are intended for manual review.
 * </p>
 */
public class FairCheckReportGenerator {
	
	public static void generateGlobalReport(String alias, Map<String, Boolean> results, String outputPath) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
            writer.write("FAIR Report for Dataset: " + alias + "\n\n");
            writer.write("=== GLOBAL CHECKS ===\n");

            for (Map.Entry<String, Boolean> entry : results.entrySet()) {
                writer.write("[" + entry.getKey() + "] " + (entry.getValue() ? "PASSED" : "FAILED") + "\n");
            }

            writer.write("\n");
            System.out.println("Global report written to: " + outputPath);
        } catch (IOException e) {
            System.err.println("Error writing global report: " + e.getMessage());
        }
    }

    public static void generateColumnReport(String alias, Map<String, Map<String, List<FairCheckResult>>> results, String outputPath) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath, true))) {
            writer.write("=== COLUMN CHECKS ===\n");

            for (Map.Entry<String, Map<String, List<FairCheckResult>>> columnEntry : results.entrySet()) {
                String columnName = columnEntry.getKey();
                writer.write("\nColumn: " + columnName + "\n");

                Map<String, List<FairCheckResult>> categorized = columnEntry.getValue();
                for (Map.Entry<String, List<FairCheckResult>> categoryEntry : categorized.entrySet()) {
                    List<FairCheckResult> checkResults = categoryEntry.getValue();

                    for (FairCheckResult result : checkResults) {
                        writer.write("  [" + result.getCheckId() + "] " + result.getDescription() + ": " + (result.isPassed() ? "PASSED" : "FAILED") + "\n");
                        if (!result.isPassed() && result.getInvalidRows() != null && !result.getInvalidRows().isEmpty()) {
                            writer.write("    Invalid rows:\n");
                            for (String issue : result.getInvalidRows()) {
                                writer.write("      - " + issue + "\n");
                            }
                        }
                    }
                }
            }

            writer.write("\n");
            System.out.println("Column report appended to: " + outputPath);
        } catch (IOException e) {
            System.err.println("Error writing column report: " + e.getMessage());
        }
    }
}
