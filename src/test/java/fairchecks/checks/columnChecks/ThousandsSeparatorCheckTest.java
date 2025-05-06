package fairchecks.checks.columnChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class ThousandsSeparatorCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = ColumnHelperClass.setUpFacade();
	}
	
	@Test
	public void thousandsSeparatorCorrectTest() throws Exception {
		
		Dataset<Row> dataset = ColumnHelperClass.getDataset(facade, "frame1");
		
		String[] columnsToCheck = new String[] { "quantity" };
		
		for (String column : columnsToCheck) {
			ThousandsSeparatorCheck check = new ThousandsSeparatorCheck(column);
			boolean result = check.executeCheck(dataset);

			assertTrue("Expected column '" + column + "' to pass thousands separator check.", result);
		}
	}
	
	@Test
	public void thousandsSeparatorWrongTest() throws Exception {
		
		Dataset<Row> dataset = ColumnHelperClass.getDataset(facade, "frame2");
		
		String[] columnsToCheck = new String[] { "quantity" };
		
		for (String column : columnsToCheck) {
			ThousandsSeparatorCheck check = new ThousandsSeparatorCheck(column);
			boolean result = check.executeCheck(dataset);

			assertFalse("Expected column '" + column + "' to fail thousands separator check.", result);
		}
	}
}
