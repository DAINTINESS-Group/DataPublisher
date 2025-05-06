package fairchecks.checks.globalChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class CsvColumnHeaderCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = GlobalHelperClass.setUpFacade();
	}
	
	@Test
	public void columnHeaderCorrectTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame3");
		
		CsvColumnHeaderCheck check1 = new CsvColumnHeaderCheck();
		boolean result1 = check1.executeCheck(dataset);
		assertTrue("Expected header in first row dataset to pass.", result1);
	}
	
	@Test
	public void columnHeaderWrongTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame6");
		
		CsvColumnHeaderCheck check1 = new CsvColumnHeaderCheck();
		boolean result2 = check1.executeCheck(dataset);
		assertFalse("Expected header not in first row dataset to fail.", result2);
	}
}
