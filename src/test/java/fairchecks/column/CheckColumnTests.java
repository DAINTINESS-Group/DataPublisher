package fairchecks.column;

import static org.junit.Assert.*;

import java.lang.reflect.Method;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import engine.*;
import fairchecks.checks.columnChecks.*;
import model.DatasetProfile;


public class CheckColumnTests {
	
	IDataPublisherFacade facade;

    @Before
    public void setUp()
    {
        FacadeFactory facadeFactory = new FacadeFactory();

        facade = facadeFactory.createDataPublisherFacade();
        facade.registerDataset("src\\test\\resources\\datasets\\fruits_test.csv", "frame1", true);
        facade.registerDataset("src\\test\\resources\\datasets\\fruits_test_wrong.csv", "frame2", true);
        
    }
    
    @Test
    public void nullValueCheckTest()
    {
    	try 
    	{
    		Method method = DataPublisherFacade.class.getDeclaredMethod("getProfile", String.class);
            method.setAccessible(true);
        	DatasetProfile profile = (DatasetProfile) method.invoke(facade, "frame2");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            String[] columnsToCheck = new String[] { "Fruit", "price", "color" };

            for (String column : columnsToCheck) {
                //System.out.println("Checking column: " + column);

                NullValueMarkingCheck check = new NullValueMarkingCheck(column);
                boolean result = check.executeCheck(dataset);

                if (column.equals("price")) {
                	assertFalse("Expected column '" + column + "' to fail null check.", result);
                	//check.getInvalidRows().forEach(System.out::println);
                } else {
                	assertTrue("Expected column '" + column + "' to pass null check.", result);
                }
            }
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    }
    
    @Test
    public void specialCharacterCheckTest()
    {
    	try 
    	{
    		Method method = DataPublisherFacade.class.getDeclaredMethod("getProfile", String.class);
            method.setAccessible(true);
        	DatasetProfile profile = (DatasetProfile) method.invoke(facade, "frame2");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            String[] columnsToCheck = new String[] { "Fruit", "price", "color" };

            for (String column : columnsToCheck) {
                //System.out.println("Checking column: " + column);

                SpecialCharacterCheck check = new SpecialCharacterCheck(column);
                boolean result = check.executeCheck(dataset);

                if (column.equals("price")) {
                	assertFalse("Expected column '" + column + "' to fail special character check.", result);
                	//check.getInvalidRows().forEach(System.out::println);
                } else {
                	assertTrue("Expected column '" + column + "' to pass special character check.", result);
                }
            }
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    }
    
    @Test
    public void dataAccuracyCheckTest()
    {
    	try 
    	{
    		Method method = DataPublisherFacade.class.getDeclaredMethod("getProfile", String.class);
            method.setAccessible(true);
        	DatasetProfile profile = (DatasetProfile) method.invoke(facade, "frame1");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            String[] columnsToCheck = new String[] { "price", "color", "currency" };

            for (String column : columnsToCheck) {
                //System.out.println("Checking column: " + column);

                DataAccuracyCheck check = new DataAccuracyCheck(column);
                boolean result = check.executeCheck(dataset);

                if (column.equals("currency")) {
                	assertTrue("Expected column '" + column + "' to pass data accuracy check.", result);
                } else {
                	assertFalse("Expected column '" + column + "' to fail data accuracy check.", result);
                	//check.getInvalidRows().forEach(System.out::println);
                }
            }
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    }
    
    @Test
    public void thousandsSeparatorCheckTest()
    {
    	try 
    	{
    		Method method = DataPublisherFacade.class.getDeclaredMethod("getProfile", String.class);
            method.setAccessible(true);
        	DatasetProfile profile = (DatasetProfile) method.invoke(facade, "frame1");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            String[] columnsToCheck = new String[] { "quantity" };

            for (String column : columnsToCheck) {
                ThousandsSeparatorCheck check = new ThousandsSeparatorCheck(column);
                boolean result = check.executeCheck(dataset);

                assertTrue("Expected column '" + column + "' to pass thousands separator check.", result);
            }
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    	
    }
    
    @Test
    public void thousandsSeparatorCheckWrongTest()
    {
    	try 
    	{
    		Method method = DataPublisherFacade.class.getDeclaredMethod("getProfile", String.class);
            method.setAccessible(true);
        	DatasetProfile profile = (DatasetProfile) method.invoke(facade, "frame2");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            String[] columnsToCheck = new String[] { "quantity" };

            for (String column : columnsToCheck) {
            	ThousandsSeparatorCheck check = new ThousandsSeparatorCheck(column);
                boolean result = check.executeCheck(dataset);

                assertFalse("Expected column '" + column + "' to fail thousands separator check.", result);
                //check.getInvalidRows().forEach(System.out::println);
            }
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    }
    
    @Test
    public void decimalFormatCheckTest()
    {
    	try 
    	{
    		Method method = DataPublisherFacade.class.getDeclaredMethod("getProfile", String.class);
            method.setAccessible(true);
        	DatasetProfile profile = (DatasetProfile) method.invoke(facade, "frame1");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            String[] columnsToCheck = new String[] { "price" };

            for (String column : columnsToCheck) {
                DecimalFormatCheck check = new DecimalFormatCheck(column);
                boolean result = check.executeCheck(dataset);

                assertTrue("Expected column '" + column + "' to pass decimal format check.", result);
            }
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    	
    }
    
    @Test
    public void decimalFormatCheckWrongTest()
    {
    	try 
    	{
    		Method method = DataPublisherFacade.class.getDeclaredMethod("getProfile", String.class);
            method.setAccessible(true);
        	DatasetProfile profile = (DatasetProfile) method.invoke(facade, "frame2");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            String[] columnsToCheck = new String[] { "price" };

            for (String column : columnsToCheck) {
            	DecimalFormatCheck check = new DecimalFormatCheck(column);
                boolean result = check.executeCheck(dataset);

                assertFalse("Expected column '" + column + "' to fail decimal format check.", result);
                //check.getInvalidRows().forEach(System.out::println);
            }
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    }
    
    @Test
    public void dateTimeFormatCheckTest()
    {
    	try 
    	{
    		Method method = DataPublisherFacade.class.getDeclaredMethod("getProfile", String.class);
            method.setAccessible(true);
        	DatasetProfile profile = (DatasetProfile) method.invoke(facade, "frame1");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            String[] columnsToCheck = new String[] { "purchase timestamp" };

            for (String column : columnsToCheck) {
                DateTimeFormatCheck check = new DateTimeFormatCheck(column);
                boolean result = check.executeCheck(dataset);

                assertTrue("Expected column '" + column + "' to pass date time format check.", result);
            }
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    	
    }
    
    @Test
    public void dateTimeFormatCheckWrongTest()
    {
    	try 
    	{
    		Method method = DataPublisherFacade.class.getDeclaredMethod("getProfile", String.class);
            method.setAccessible(true);
        	DatasetProfile profile = (DatasetProfile) method.invoke(facade, "frame2");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            String[] columnsToCheck = new String[] { "purchase timestamp" };

            for (String column : columnsToCheck) {
            	DateTimeFormatCheck check = new DateTimeFormatCheck(column);
                boolean result = check.executeCheck(dataset);

                assertFalse("Expected column '" + column + "' to fail date time format check.", result);
                //check.getInvalidRows().forEach(System.out::println);
            }
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    }
    
    @Test
    public void controlledVocabularyCheckTest()
    {
    	try 
    	{
    		Method method = DataPublisherFacade.class.getDeclaredMethod("getProfile", String.class);
            method.setAccessible(true);
        	DatasetProfile profile = (DatasetProfile) method.invoke(facade, "frame1");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            String[] columnsToCheck = new String[] { "currency" };

            for (String column : columnsToCheck) {
                ControlledVocabularyCheck check = new ControlledVocabularyCheck(column);
                boolean result = check.executeCheck(dataset);

                assertTrue("Expected column '" + column + "' to pass controlled vocabulary check.", result);
            }
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    	
    }
    
    @Test
    public void controlledVocabularyCheckWrongTest()
    {
    	try 
    	{
    		Method method = DataPublisherFacade.class.getDeclaredMethod("getProfile", String.class);
            method.setAccessible(true);
        	DatasetProfile profile = (DatasetProfile) method.invoke(facade, "frame2");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            String[] columnsToCheck = new String[] { "currency" };

            for (String column : columnsToCheck) {
            	ControlledVocabularyCheck check = new ControlledVocabularyCheck(column);
                boolean result = check.executeCheck(dataset);

                assertFalse("Expected column '" + column + "' to fail controlled vocabulary check.", result);
                //check.getInvalidRows().forEach(System.out::println);
            }
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    }
    
    @Test
    public void csvUnitInHeaderCheckTest()
    {
    	try 
    	{
    		Method method = DataPublisherFacade.class.getDeclaredMethod("getProfile", String.class);
            method.setAccessible(true);
        	DatasetProfile profile = (DatasetProfile) method.invoke(facade, "frame1");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            String[] columnsToCheck = new String[] { "weight (g)" };

            for (String column : columnsToCheck) {
                CsvUnitInHeaderCheck check = new CsvUnitInHeaderCheck(column);
                boolean result = check.executeCheck(dataset);

                assertTrue("Expected column '" + column + "' to pass unit in header check.", result);
            }
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    	
    }
    
    @Test
    public void csvUnitInHeaderCheckWrongTest()
    {
    	try 
    	{
    		Method method = DataPublisherFacade.class.getDeclaredMethod("getProfile", String.class);
            method.setAccessible(true);
        	DatasetProfile profile = (DatasetProfile) method.invoke(facade, "frame2");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            String[] columnsToCheck = new String[] { "weight" };

            for (String column : columnsToCheck) {
            	CsvUnitInHeaderCheck check = new CsvUnitInHeaderCheck(column);
                boolean result = check.executeCheck(dataset);

                assertFalse("Expected column '" + column + "' to fail unit in header check.", result);
                check.getInvalidRows().forEach(System.out::println);
            }
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    }
    
}
