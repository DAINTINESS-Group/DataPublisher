package engine;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;


import model.DatasetProfile;
import model.FairCheckResult;
import utils.RegistrationResponse;
import utils.ReportType;

public class FacadeTests {
	
	IDataPublisherFacade facade;

    @Before
    public void setUp()
    {
		FacadeFactory facadeFactory = new FacadeFactory();
		
		facade = facadeFactory.createDataPublisherFacade();
		facade.registerDataset("src\\test\\resources\\datasets\\fruits_test.csv", "frame1", true);
		facade.registerDataset("src\\test\\resources\\datasets\\fruits_test_wrong.csv", "frame2", true);
		facade.registerDataset("src\\test\\resources\\datasets\\students_test.csv", "frame3", true);
		facade.registerDataset("src\\test\\resources\\datasets\\students_test_wrong2.csv", "frame5", true);
    }
    
    @Test
    public void registerDatasetTest()
    {
		RegistrationResponse response = facade.registerDataset("src/test/resources/datasets/countries.csv", "testDataset", true);
		assertEquals(RegistrationResponse.SUCCESS, response);
		
		DatasetProfile profile = facade.getProfile("frame1");
		assertNotNull(profile);
		assertEquals("frame1", profile.getAlias());
		assertEquals("src\\test\\resources\\datasets\\fruits_test.csv", profile.getFilePath());
    }
    
    @Test
    public void executeGlobalChecksTest() {
    	Map<String, Boolean> globalResultsCorrect = facade.executeGlobalChecks("frame3");
    	
    	assertNotNull(globalResultsCorrect);
    	
    	String expectedFailingCheck = "IEU9 - CSV - Data shall use the semicolon, not the comma, as a separator between values, with no spaces or tabs on either side.";
    	for (Map.Entry<String, Boolean> entry : globalResultsCorrect.entrySet()) {
            if (entry.getKey().equals(expectedFailingCheck)) {
                assertFalse("Expected check to fail: " + entry.getKey(), entry.getValue());
            } else {
                assertTrue("Expected check to pass: " + entry.getKey(), entry.getValue());
            }
        }
    	
    	Map<String, Boolean> globalResultsWrong = facade.executeGlobalChecks("frame5");
        
    	assertFalse(globalResultsWrong.get("IEU6 - Once tags are referenced by unique identifiers from controlled vocabularies, URI details shall be retrieved in the data."));
    	assertFalse(globalResultsWrong.get("IEU7 - Data shall leverage URIs and be published as Linked Data in the form of semantic characters."));
    	assertFalse(globalResultsWrong.get("IEU16 - CSV - Data files should have the same number of columns for all rows."));
    	assertFalse(globalResultsWrong.get("IEU11.3 - CSV - Files should contain only one header line."));
    	assertFalse(globalResultsWrong.get("IEU11.1 - CSV - Data should not contain explanations, dates, modifications, sheet names, etc."));
    }
    
    @Test
    public void executeGlobalCheckByIdTest() {
    	Map<String, Boolean> globalResultsCorrect = facade.executeGlobalChecks("frame3", "IEU16");
    	
    	assertNotNull(globalResultsCorrect);
    	assertEquals(1, globalResultsCorrect.size());
    	assertTrue(globalResultsCorrect.get("IEU16 - CSV - Data files should have the same number of columns for all rows."));
    }
    
    @Test
    public void executeColumnChecksTest() {
    	Map<String, Map<String, List<FairCheckResult>>> columnResults1 = facade.executeColumnChecks("frame1");
    	
    	assertNotNull(columnResults1);
    	assertTrue(columnResults1.containsKey("currency"));
    	
    	Map<String, List<FairCheckResult>> currencyResults = columnResults1.get("currency");
    	List<FairCheckResult> reusabilityChecks = currencyResults.get("Reusability");

        assertFalse(reusabilityChecks.isEmpty());
        for (FairCheckResult result : reusabilityChecks) {
            if (result.getCheckId().equals("REU4")) {
                assertTrue(result.isPassed());
            }
        }
    	
    	Map<String, Map<String, List<FairCheckResult>>> columnResults2 = facade.executeColumnChecks("frame2");
    	
    	assertNotNull(columnResults2);
    	assertTrue(columnResults2.containsKey("Fruit"));
    	assertTrue(columnResults2.containsKey("price"));
    	
    	Map<String, List<FairCheckResult>> fruitResults = columnResults2.get("Fruit");
    	Map<String, List<FairCheckResult>> priceResults = columnResults2.get("price");
    	
    	List<FairCheckResult> findabilityFruitChecks = fruitResults.get("Findability");
    	List<FairCheckResult> findabilityPriceChecks = priceResults.get("Findability");
        assertFalse(findabilityFruitChecks.isEmpty());
        assertFalse(findabilityPriceChecks.isEmpty());
        
        for (FairCheckResult result : findabilityFruitChecks) {
            if (result.getCheckId().equals("FEU2")) {
                assertTrue(result.isPassed());
            }
        }
        for (FairCheckResult result : findabilityPriceChecks) {
            if (result.getCheckId().equals("FEU2")) {
                assertFalse(result.isPassed());
            }
        }
        
        List<FairCheckResult> interoperabilityFruitChecks = fruitResults.get("Interoperability");
    	List<FairCheckResult> interoperabilityPriceChecks = priceResults.get("Interoperability");
        assertFalse(interoperabilityFruitChecks.isEmpty());
        assertFalse(interoperabilityPriceChecks.isEmpty());
        
        for (FairCheckResult result : interoperabilityFruitChecks) {
            if (result.getCheckId().equals("IEU3.2")) {
                assertTrue(result.isPassed());
            }
        }
        for (FairCheckResult result : interoperabilityPriceChecks) {
            if (result.getCheckId().equals("IEU3.2")) {
                assertFalse(result.isPassed());
            }
        }
    }
    
    @Test
    public void executeColumnChecksByIdTest() {
    	Map<String, Map<String, List<FairCheckResult>>> columnResults = facade.executeColumnChecks("frame1", "all", "FEU2");
    	
    	assertNotNull("Result map should not be null", columnResults);
    	assertFalse("Expected results for multiple columns", columnResults.isEmpty());
    	
    	for (Map.Entry<String, Map<String, List<FairCheckResult>>> columnEntry : columnResults.entrySet()) {
    		String columnName = columnEntry.getKey();
    		Map<String, List<FairCheckResult>> checkMap = columnEntry.getValue();
    		assertNotNull("Check map should not be null for column: " + columnName, checkMap);
    		
    		List<FairCheckResult> results = checkMap.get("Findability");
    		assertNotNull("Check results should not be null for column: " + columnName, results);
    		assertFalse("Expected at least one result for column: " + columnName, results.isEmpty());
    		
    		for (FairCheckResult result : results) {
    			assertEquals("Check ID mismatch in result", "FEU2", result.getCheckId());
    			
    			assertTrue("Expected check FEU2 to pass for column: " + columnName, result.isPassed());
    		}
    	}
    }
    
    @Test
    public void executeSpecificCheckInSpecificColumnTest() {
    	Map<String, Map<String, List<FairCheckResult>>> columnResults = facade.executeColumnChecks("frame1", "color", "FEU2");
    	
    	assertNotNull("Result map should not be null", columnResults);
    	assertEquals("Expected results for only the 'color' column", 1, columnResults.size());
    	assertTrue("Expected results for column 'color'", columnResults.containsKey("color"));
    	
    	Map<String, List<FairCheckResult>> checkResultsMap = columnResults.get("color");
    	assertNotNull("Check results map should not be null for column 'color'", checkResultsMap);
    	assertEquals("Expected only one check result for column 'color'", 1, checkResultsMap.size());
    	
    	List<FairCheckResult> results = checkResultsMap.get("Findability");
    	assertNotNull("Check result list should not be null for 'Findability'", results);
    	
    	for (FairCheckResult result : results) {
    		assertEquals("Unexpected check ID in result", "FEU2", result.getCheckId());
    		assertTrue("Expected check FEU2 to pass on column 'color'", result.isPassed());
    	}
    }
    
    @Test
    public void executeChecksinSpecificColumnTest() {
    	Map<String, Map<String, List<FairCheckResult>>> columnResults = facade.executeColumnChecks("frame1", "color", "all");
    	
    	assertNotNull("Result map should not be null", columnResults);
    	assertEquals("Expected results for only the 'color' column", 1, columnResults.size());
    	assertTrue("Expected results for column 'color'", columnResults.containsKey("color"));
    	
    	Map<String, List<FairCheckResult>> checkResultsMap = columnResults.get("color");
    	assertNotNull("Check results map should not be null for column 'color'", checkResultsMap);
    	
    	List<FairCheckResult> interoperabiltyResults = checkResultsMap.get("Interoperability");
    	assertNotNull("Result list for 'Interoperability' should not be null", interoperabiltyResults);
    	
    	for (FairCheckResult result : interoperabiltyResults) {
    		assertNotNull("Check ID should not be null", result.getCheckId());
    	}
    }
    
    @Test
    public void generateGlobalReportTest() throws Exception {
		String globalReportPath = "src/test/resources/reports/GlobalReportTest.txt";
		Map<String, Boolean> globalResults = facade.executeGlobalChecks("frame1");
		ReportType reportTypeTXT = ReportType.TEXT;
		
		facade.generateGlobalReport("frame1", globalResults, globalReportPath, reportTypeTXT);
		
		File reportFile = new File(globalReportPath);
		assertTrue("Global report file should exist", reportFile.exists());
		assertTrue("Global report file should not be empty", reportFile.length() > 0);
    }
    
    @Test
    public void generateColumnReportTest() throws Exception {
		String columnReportPath = "src/test/resources/reports/ColumnReportTest.txt";
		Map<String, Map<String, List<FairCheckResult>>> columnResults = facade.executeColumnChecks("frame1");
		ReportType reportTypeTXT = ReportType.TEXT;
		
		facade.generateColumnReport("frame1", columnResults, columnReportPath, reportTypeTXT);
		
		File reportFile = new File(columnReportPath);
		assertTrue("Column report file should exist", reportFile.exists());
		assertTrue("Column report file should not be empty", reportFile.length() > 0);
    }
}
