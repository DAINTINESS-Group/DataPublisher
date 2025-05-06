package fairchecks.checks.globalChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class CsvSingleHeaderCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = GlobalHelperClass.setUpFacade();
	}
	
	@Test
	public void singleHeaderCorrectTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame3");
		
		CsvSingleHeaderCheck check1 = new CsvSingleHeaderCheck();
		boolean result1 = check1.executeCheck(dataset);
		assertTrue("Expected one header row in this dataset.", result1);
	}
	
	@Test
	public void singleHeaderWrongTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame5");
		
		CsvSingleHeaderCheck check2 = new CsvSingleHeaderCheck();
		boolean result2 = check2.executeCheck(dataset);
		assertFalse("Expected first and second rows as header in this dataset.", result2);
	}
}
