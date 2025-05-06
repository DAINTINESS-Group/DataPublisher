package fairchecks.checks.globalChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class SeparateSheetDatasetCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = GlobalHelperClass.setUpFacade();
	}
	
	@Test
	public void separateSheetDatasetCorrectTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame7");
		
		SeparateSheetDatasetCheck check1 = new SeparateSheetDatasetCheck();
		boolean result1 = check1.executeCheck(dataset);
		assertTrue("Expected accessible dataset to pass.", result1);
	}
	
	@Test
	public void separateSheetDatasetWrongTest() throws Exception {
		
		Dataset<Row> dataset = GlobalHelperClass.getDataset(facade, "frame8");
		
		SeparateSheetDatasetCheck check1 = new SeparateSheetDatasetCheck();
		boolean result2 = check1.executeCheck(dataset);
		assertFalse("Expected two sheet dataset to fail.", result2);
	}
}
