package report;

import model.FairCheckResult;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

import static org.junit.Assert.*;

public class MarkdownChecksReportGeneratorTest {
	
	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();
	
	private final MarkdownChecksReportGenerator generator = new MarkdownChecksReportGenerator();
	
	@Test
	public void testGenerateGlobalReportValidMarkdownOutput() throws IOException {
		Map<String, Boolean> results = new LinkedHashMap<>();
		results.put("Check A", true);
		results.put("Check B", false);
		
		File outputFile = tempFolder.newFile("globalReport.md");
		generator.generateGlobalReport("TestDataset", results, outputFile.getAbsolutePath());
		
		String content = new String(Files.readAllBytes(outputFile.toPath()));
		
		assertTrue(content.contains("# FAIR Report for Dataset: TestDataset"));
		assertTrue(content.contains("**Check A**: `PASSED`"));
		assertTrue(content.contains("**Check B**: `FAILED`"));
	}
	
	@Test
	public void testGenerateColumnReportValidMarkdownOutput() throws IOException {
		List<FairCheckResult> checkResults = new ArrayList<>();
		checkResults.add(new FairCheckResult("C001", "Check description 1", true, Collections.emptyList()));
		checkResults.add(new FairCheckResult("C002", "Check description 2", false, Arrays.asList("Row 2", "Row 5")));
		
		Map<String, List<FairCheckResult>> checksByCategory = new HashMap<>();
		checksByCategory.put("Quality", checkResults);
		
		Map<String, Map<String, List<FairCheckResult>>> results = new LinkedHashMap<>();
		results.put("ColumnA", checksByCategory);
		
		File outputFile = tempFolder.newFile("columnReport.md");
		generator.generateColumnReport("TestDataset", results, outputFile.getAbsolutePath());
		
		String content = new String(Files.readAllBytes(outputFile.toPath()));
		
		assertTrue(content.contains("### Column: `ColumnA`"));
		assertTrue(content.contains("**[C001]** Check description 1: `PASSED`"));
		assertTrue(content.contains("**[C002]** Check description 2: `FAILED`"));
		assertTrue(content.contains("- Invalid rows:"));
		assertTrue(content.contains("    - Row 2"));
		assertTrue(content.contains("    - Row 5"));
	}
}
