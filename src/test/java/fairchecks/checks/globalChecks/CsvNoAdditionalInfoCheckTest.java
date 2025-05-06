package fairchecks.checks.globalChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class CsvNoAdditionalInfoCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = GlobalHelperClass.setUpFacade();
	}
	
	@Test
	public void noAdditionalInfoCorrectTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame3");
		
		CsvNoAdditionalInfoCheck check1 = new CsvNoAdditionalInfoCheck();
		boolean result = check1.executeCheck(dataset);
		assertTrue("Expected one header row in this dataset.", result);
	}
	
	@Test
	public void noAdditionalInfoWrongTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame5");
		
		CsvNoAdditionalInfoCheck check1 = new CsvNoAdditionalInfoCheck();
		boolean result = check1.executeCheck(dataset);
		assertFalse("Expected first and second rows as header in this dataset.", result);
	}
}
