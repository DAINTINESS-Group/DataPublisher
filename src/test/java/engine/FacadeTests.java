package engine;

import static org.junit.Assert.*;

import java.util.Map;

import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.Test;

import model.DatasetProfile;

public class FacadeTests {
	
	IDataPublisherFacade facade;

    @Before
    public void setUp()
    {
        FacadeFactory facadeFactory = new FacadeFactory();

        facade = facadeFactory.createDataPublisherFacade();
        facade.registerDataset("src\\test\\resources\\datasets\\countries.csv", "frame1", true);
        //facade.registerDataset("src\\test\\resources\\datasets\\cars_100_tests.csv", "frame2", true);
        
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
    public void globalCheckExecutionTest()
    {
    	try
    	{
    		DataPublisherFacade concreteFacade = (DataPublisherFacade) facade;
            Map<String, Boolean> results = concreteFacade.executeGlobalChecks("frame1");

            assertNotNull(results);
            assertFalse("FAIR check results should not be empty", results.isEmpty());
            assertTrue("FAIR Check Results should contain IEU3.1", 
                    results.containsKey("IEU3.1 - The encoding of choice on the web is UTF-8, and must be explicitly enabled on ‘Save As’."));
            assertTrue("UTF-8 dataset should pass the UTF-8 encoding check",
                results.get("IEU3.1 - The encoding of choice on the web is UTF-8, and must be explicitly enabled on ‘Save As’."));
            
            System.out.println("UTF-8 Encoding Check Test Results:");
            results.forEach((question, passed) -> 
                System.out.println(question + ": " + (passed ? "Passed" : "Failed"))
            );
    	}
    	catch(Exception e ){
    		e.printStackTrace();
            fail("Exception occurred while executing global FAIR checks.");
    	}
    }

}
