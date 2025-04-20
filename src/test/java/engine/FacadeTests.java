package engine;

import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import model.DatasetProfile;
import model.FairCheckResult;

public class FacadeTests {
	
	IDataPublisherFacade facade;

    @Before
    public void setUp()
    {
        FacadeFactory facadeFactory = new FacadeFactory();

        facade = facadeFactory.createDataPublisherFacade();
        facade.registerDataset("src\\test\\resources\\datasets\\fruits_test.csv", "frame1", true);
        facade.registerDataset("src\\test\\resources\\datasets\\fruits_test_wrong.csv", "frame2", true);
        facade.registerDataset("src\\test\\resources\\datasets\\students_test.csv", "frame3", true);
        facade.registerDataset("src\\test\\resources\\datasets\\students_test_wrong2.csv", "frame5", true);
    }
    
    @Test
    public void profileCreationTest()
    {
        try
        {
            Method method = DataPublisherFacade.class.getDeclaredMethod("getProfile", String.class);
            method.setAccessible(true);
            assertNotNull((DatasetProfile) method.invoke(facade, "frame1"));
            assertNull((DatasetProfile) method.invoke(facade, "notFrame"));
        }
        catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    }
    
    @Test
    public void executeGlobalChecksTest() {
    	Map<String, Boolean> globalResultsCorrect = facade.executeGlobalChecks("frame3");
    	
    	assertNotNull(globalResultsCorrect);
    	for (Map.Entry<String, Boolean> entry : globalResultsCorrect.entrySet()) {
            assertTrue("Expected check to pass: " + entry.getKey(), entry.getValue());
        }
    	
    	Map<String, Boolean> globalResultsWrong = facade.executeGlobalChecks("frame5");
        
    	assertFalse(globalResultsWrong.get("IEU6 - Once tags are referenced by unique identifiers from controlled vocabularies, URI details shall be retrieved in the data."));
    	assertFalse(globalResultsWrong.get("IEU7 - Data shall leverage URIs and be published as Linked Data in the form of semantic characters."));
    	assertFalse(globalResultsWrong.get("IEU16 - CSV - Data files should have the same number of columns for all rows."));
    	assertFalse(globalResultsWrong.get("IEU11.3 - CSV - Files should contain only one header line."));
    	assertFalse(globalResultsWrong.get("IEU11.1 - CSV - Data should not contain explanations, dates, modifications, sheet names, etc."));
    }
    
    @Test
    public void executeColumnChecksTest() {
    	Map<String, Map<String, List<FairCheckResult>>> columnResults1 = facade.executeColumnChecks("frame1");
    	
    	assertNotNull(columnResults1);
    	//Map<String, List<FairCheckResult>> currencyResults = columnResults1.get("ISO");
    	
    	Map<String, Map<String, List<FairCheckResult>>> columnResults2 = facade.executeColumnChecks("frame2");
    	
    	assertNotNull(columnResults2);
    	//Map<String, List<FairCheckResult>> fruitResults = columnResults2.get("fruit");
    	//Map<String, List<FairCheckResult>> priceResults = columnResults2.get("price");
    }
}
