package engine;

import static org.junit.Assert.*;

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
}
