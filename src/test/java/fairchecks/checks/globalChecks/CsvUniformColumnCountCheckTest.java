package fairchecks.checks.globalChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class CsvUniformColumnCountCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = GlobalHelperClass.setUpFacade();
	}
	
	@Test
	public void uniformColumnCountCorrectTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame3");
		
		CsvUniformColumnCountCheck check1 = new CsvUniformColumnCountCheck();
		boolean result1 = check1.executeCheck(dataset);
		assertTrue("Expected equal number of rows and columns.", result1);
	}
	
	@Test
	public void uniformColumnCountWrongTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame5");
		
		CsvUniformColumnCountCheck check2 = new CsvUniformColumnCountCheck();
		boolean result2 = check2.executeCheck(dataset);
		assertFalse("Expected more columns in some rows.", result2);
	}
}
