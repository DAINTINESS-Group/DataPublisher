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

public class TxtChecksReportGeneratorTest {
	
	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();
	
	private final TxtChecksReportGenerator generator = new TxtChecksReportGenerator();
	
	@Test
	public void testGenerateGlobalReportValidTxtOutput() throws IOException {
		Map<String, Boolean> results = new LinkedHashMap<>();
		results.put("Check A", true);
		results.put("Check B", false);
		
		File outputFile = tempFolder.newFile("globalReport.txt");
		generator.generateGlobalReport("MyDataset", results, outputFile.getAbsolutePath());
		
		String content = new String(Files.readAllBytes(outputFile.toPath()));
		
		assertTrue(content.contains("FAIR Report for Dataset: MyDataset"));
		assertTrue(content.contains("[Check A] PASSED"));
		assertTrue(content.contains("[Check B] FAILED"));
	}
	
	@Test
	public void testGenerateColumnReportValidTxtOutput() throws IOException{
		List<FairCheckResult> checkResults = new ArrayList<>();
		checkResults.add(new FairCheckResult("C001", "Description 1", true, Collections.emptyList()));
		checkResults.add(new FairCheckResult("C002", "Description 2", false, Arrays.asList("Row 1", "Row 3")));
		
		Map<String, List<FairCheckResult>> categoryMap = new HashMap<>();
		categoryMap.put("Integrity", checkResults);
		
		Map<String, Map<String, List<FairCheckResult>>> results = new LinkedHashMap<>();
		results.put("ColumnX", categoryMap);
		
		File outputFile = tempFolder.newFile("columnReport.txt");
		generator.generateColumnReport("MyDataset", results, outputFile.getAbsolutePath());
		
		String content = new String(Files.readAllBytes(outputFile.toPath()));
		
		assertTrue(content.contains("=== COLUMN CHECKS ==="));
		assertTrue(content.contains("Column: ColumnX"));
		assertTrue(content.contains("[C001] Description 1: PASSED"));
		assertTrue(content.contains("[C002] Description 2: FAILED"));
		assertTrue(content.contains("Invalid rows:"));
		assertTrue(content.contains("- Row 1"));
		assertTrue(content.contains("- Row 3"));
	}

}
