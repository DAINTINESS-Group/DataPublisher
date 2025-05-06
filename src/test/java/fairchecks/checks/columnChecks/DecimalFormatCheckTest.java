package fairchecks.checks.columnChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class DecimalFormatCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = ColumnHelperClass.setUpFacade();
	}
	
	@Test
	public void decimalFormatCorrectTest() throws Exception {
		
		Dataset<Row> dataset = ColumnHelperClass.getDataset(facade, "frame1");
		
		String[] columnsToCheck = new String[] { "price" };
		
		for (String column : columnsToCheck) {
			DecimalFormatCheck check = new DecimalFormatCheck(column);
			boolean result = check.executeCheck(dataset);
			
			assertTrue("Expected column '" + column + "' to pass decimal format check.", result);
		}
	}
	
	@Test
	public void decimalFormatWrongTest() throws Exception {
		
		Dataset<Row> dataset = ColumnHelperClass.getDataset(facade, "frame2");
		
		String[] columnsToCheck = new String[] { "price" };
		
		for (String column : columnsToCheck) {
			DecimalFormatCheck check = new DecimalFormatCheck(column);
			boolean result = check.executeCheck(dataset);
			
			assertFalse("Expected column '" + column + "' to fail decimal format check.", result);
		}
	}
}
