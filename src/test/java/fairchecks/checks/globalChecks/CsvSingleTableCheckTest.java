package fairchecks.checks.globalChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class CsvSingleTableCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = GlobalHelperClass.setUpFacade();
	}
	
	@Test
	public void singleTableCorrectTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame3");
		
		CsvSingleTableCheck check1 = new CsvSingleTableCheck();
		boolean result1 = check1.executeCheck(dataset);
		assertTrue("Expected one table dataset to pass.", result1);
	}
	
	@Test
	public void singleTableWrongTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame4");
		
		CsvSingleTableCheck check2 = new CsvSingleTableCheck();
		boolean result2 = check2.executeCheck(dataset);
		assertFalse("Expected two tables dataset to fail.", result2);
	}
}
