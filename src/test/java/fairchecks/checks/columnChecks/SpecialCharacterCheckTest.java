package fairchecks.checks.columnChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class SpecialCharacterCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = ColumnHelperClass.setUpFacade();
	}
	
	@Test
	public void specialCharacterTest() throws Exception {
		Dataset<Row> dataset = ColumnHelperClass.getDataset(facade, "frame2");
		
		String[] columnsToCheck = { "Fruit", "price", "color" };
		
		for (String column : columnsToCheck) {
			SpecialCharacterCheck check = new SpecialCharacterCheck(column);
			boolean result = check.executeCheck(dataset);
			
			if ("price".equals(column)) {
				assertFalse("Expected column '" + column + "' to fail special character check.", result);
			} else {
				assertTrue("Expected column '" + column + "' to pass special character check.", result);
			}
		}
	}
}
