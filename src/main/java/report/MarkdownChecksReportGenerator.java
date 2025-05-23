package report;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import model.FairCheckResult;
/**
 * 
 * Implementation of {@link IChecksReportGenerator} that generates FAIR check reports in Markdown (.md) format.
 *
 */
public class MarkdownChecksReportGenerator implements IChecksReportGenerator {

	@Override
	public void generateGlobalReport(String alias, Map<String, Boolean> results, String outputPath) {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
			writer.write("# FAIR Report for Dataset: " + alias + "\n\n");
			writer.write("## GLOBAL CHECKS\n");
			
			for (Map.Entry<String, Boolean> entry : results.entrySet()) {
				writer.write("- **" + entry.getKey() + "**: " + (entry.getValue() ? "`PASSED`" : "`FAILED`") + "\n");
			}
			
			writer.write("\n");
			System.out.println("Markdown global report written to: " + outputPath);
		} catch (IOException e) {
			System.err.println("Error writing Markdown global report: " + e.getMessage());
		}
	}

	@Override
	public void generateColumnReport(String alias, Map<String, Map<String, List<FairCheckResult>>> results,
			String outputPath) {
		
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath, true))) {
			writer.write("## COLUMN CHECKS\n");

			for (Map.Entry<String, Map<String, List<FairCheckResult>>> columnEntry : results.entrySet()) {
				String columnName = columnEntry.getKey();
				writer.write("\n### Column: `" + columnName + "`\n");

				Map<String, List<FairCheckResult>> categorized = columnEntry.getValue();
				for (Map.Entry<String, List<FairCheckResult>> categoryEntry : categorized.entrySet()) {
					List<FairCheckResult> checkResults = categoryEntry.getValue();

					for (FairCheckResult result : checkResults) {
						writer.write("- **[" + result.getCheckId() + "]** " + result.getDescription() + ": " +
								(result.isPassed() ? "`PASSED`" : "`FAILED`") + "\n");

						if (!result.isPassed() && result.getInvalidRows() != null && !result.getInvalidRows().isEmpty()) {
							writer.write("  - Invalid rows:\n");
							for (String issue : result.getInvalidRows()) {
								writer.write("    - " + issue + "\n");
							}
						}
					}
				}
			}

			writer.write("\n");
			System.out.println("Markdown column report appended to: " + outputPath);
		} catch (IOException e) {
			System.err.println("Error writing Markdown column report: " + e.getMessage());
		}
	}

}
