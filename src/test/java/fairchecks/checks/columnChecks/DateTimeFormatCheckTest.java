package fairchecks.checks.columnChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class DateTimeFormatCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = ColumnHelperClass.setUpFacade();
	}
	
	@Test
	public void dateTimeFormatCorrectTest() throws Exception {
		
		Dataset<Row> dataset = ColumnHelperClass.getDataset(facade, "frame1");
		
		String[] columnsToCheck = new String[] { "purchase timestamp" };
		
		for (String column : columnsToCheck) {
			DateTimeFormatCheck check = new DateTimeFormatCheck(column);
			boolean result = check.executeCheck(dataset);
			
			assertTrue("Expected column '" + column + "' to pass date time format check.", result);
		}
	}
	
	@Test
	public void dateTimeFormatWrongTest() throws Exception {
		
		Dataset<Row> dataset = ColumnHelperClass.getDataset(facade, "frame2");
		
		String[] columnsToCheck = new String[] { "purchase timestamp" };
		
		for (String column : columnsToCheck) {
			DateTimeFormatCheck check = new DateTimeFormatCheck(column);
			boolean result = check.executeCheck(dataset);
			
			assertFalse("Expected column '" + column + "' to fail date time format check.", result);
		}
	}
}
