package fairchecks.checks.columnChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class NullValueMarkingCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = ColumnHelperClass.setUpFacade();
	}
	
	@Test
	public void nullValuesCheckTest() throws Exception{
		Dataset<Row> dataset = ColumnHelperClass.getDataset(facade, "frame2");
		
		String[] columnsToCheck = { "Fruit", "price", "color" };
		
		for (String column : columnsToCheck) {
			NullValueMarkingCheck check = new NullValueMarkingCheck(column);
			boolean result = check.executeCheck(dataset);
			
			if ("price".equals(column)) {
				assertFalse("Expected column '" + column + "' to fail null check.", result);
			} else {
				assertTrue("Expected column '" + column + "' to pass null check.", result);
			}
		}
	}
}
