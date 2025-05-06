package fairchecks.checks.globalChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;

import org.junit.Test;

import engine.IDataPublisherFacade;

public class DataAccessRestrictionCheckTest {
	
	private IDataPublisherFacade facade;
	
	@Before
	public void setUp() {
		facade = GlobalHelperClass.setUpFacade();
	}
	
	@Test
	public void dataAccessRestrictionCorrectTest() throws Exception {
		/*
		//Accessible Dataset
        Dataset<Row> accessibleData = GlobalHelperClass.getDataset(facade, "frame3");

        DataAccessRestrictionCheck check1 = new DataAccessRestrictionCheck();
        boolean result1 = check1.executeCheck(accessibleData);
        assertTrue("Expected accessible dataset to pass.", result1);


        //Restricted Dataset
        Dataset<Row> restrictedData = GlobalHelperClass.getDataset(facade, "frame9");
        DataAccessRestrictionCheck check2 = new DataAccessRestrictionCheck();
        boolean result2 = check2.executeCheck(restrictedData);
        assertFalse("Expected restricted dataset to fail.", result2);
        */
	}
}
