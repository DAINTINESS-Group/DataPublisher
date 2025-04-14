package fairchecks.checks.globalChecks;

import static org.junit.Assert.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import engine.*;

import model.DatasetProfile;

public class CheckGlobalTests {
	
	IDataPublisherFacade facade;

    @Before
    public void setUp()
    {
        FacadeFactory facadeFactory = new FacadeFactory();

        facade = facadeFactory.createDataPublisherFacade();
        facade.registerDataset("src\\test\\resources\\datasets\\fruits_test.csv", "frame1", true);
        facade.registerDataset("src\\test\\resources\\datasets\\fruits_test_wrong.csv", "frame2", true);
        facade.registerDataset("src\\test\\resources\\datasets\\students_test.csv", "frame3", true);
        facade.registerDataset("src\\test\\resources\\datasets\\students_test_wrong.csv", "frame4", true);
        facade.registerDataset("src\\test\\resources\\datasets\\students_test_wrong2.csv", "frame5", true);
        facade.registerDataset("src\\test\\resources\\datasets\\students_test_wrong3.csv", "frame6", false);
        facade.registerDataset("src\\test\\resources\\datasets\\excel_test.xlsx", "frame7", true);
        facade.registerDataset("src\\test\\resources\\datasets\\excel_test_wrong.xlsx", "frame8", true);
        //facade.registerDataset("src\\test\\resources\\datasets\\noAccess_test.csv", "frame9", true);
        
    }
    
    @Test
    public void columnHeaderCheckTest()
    {
    	try {
            DatasetProfile profile1 = facade.getProfile("frame3");
            Dataset<Row> oneTableData = profile1.getDataset();

            CsvColumnHeaderCheck check1 = new CsvColumnHeaderCheck();
            boolean result1 = check1.executeCheck(oneTableData);
            assertTrue("Expected header in first row dataset to pass.", result1);

        } catch (Exception e) {
            e.printStackTrace();
            fail("Test threw an exception: " + e.getMessage());
        }
    }
    
    @Test
    public void columnHeaderCheckWrongTest()
    {
    	try {
            DatasetProfile profile2 = facade.getProfile("frame6");
            Dataset<Row> twoTablesData = profile2.getDataset();
            
            CsvColumnHeaderCheck check2 = new CsvColumnHeaderCheck();
            boolean result2 = check2.executeCheck(twoTablesData);
            assertFalse("Expected header not in first row dataset to fail.", result2);

        } catch (Exception e) {
            e.printStackTrace();
            fail("Test threw an exception: " + e.getMessage());
        }
    }
    
    @Test
    public void noAdditionalInfoCheckTest()
    {
    	try {
            DatasetProfile profile1 = facade.getProfile("frame3");
            Dataset<Row> noAdditionalInfoData = profile1.getDataset();

            CsvNoAdditionalInfoCheck check1 = new CsvNoAdditionalInfoCheck();
            boolean result1 = check1.executeCheck(noAdditionalInfoData);
            assertTrue("Expected one header row in this dataset.", result1);

        } catch (Exception e) {
            e.printStackTrace();
            fail("Test threw an exception: " + e.getMessage());
        }
    }
    
    @Test
    public void noAdditionalInfoCheckWrongTest()
    {
    	try {
            DatasetProfile profile2 = facade.getProfile("frame5");
            Dataset<Row> moreInfoData = profile2.getDataset();
            
            CsvNoAdditionalInfoCheck check2 = new CsvNoAdditionalInfoCheck();
            boolean result2 = check2.executeCheck(moreInfoData);
            assertFalse("Expected first and second rows as header in this dataset.", result2);
            
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test threw an exception: " + e.getMessage());
        }
    }
    
    @Test
    public void singleHeaderCheckTest()
    {
    	try {
            DatasetProfile profile1 = facade.getProfile("frame3");
            Dataset<Row> oneHeaderData = profile1.getDataset();

            CsvSingleHeaderCheck check1 = new CsvSingleHeaderCheck();
            boolean result1 = check1.executeCheck(oneHeaderData);
            assertTrue("Expected one header row in this dataset.", result1);

        } catch (Exception e) {
            e.printStackTrace();
            fail("Test threw an exception: " + e.getMessage());
        }
    }
    
    @Test
    public void singleHeaderCheckWrongTest()
    {
    	try {
            DatasetProfile profile2 = facade.getProfile("frame5");
            Dataset<Row> secondRowData = profile2.getDataset();
            
            CsvSingleHeaderCheck check2 = new CsvSingleHeaderCheck();
            boolean result2 = check2.executeCheck(secondRowData);
            assertFalse("Expected first and second rows as header in this dataset.", result2);
            
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test threw an exception: " + e.getMessage());
        }
    }
    
    @Test
    public void singleTableCheckTest()
    {
    	try {
            DatasetProfile profile1 = facade.getProfile("frame3");
            Dataset<Row> oneTableData = profile1.getDataset();

            CsvSingleTableCheck check1 = new CsvSingleTableCheck();
            boolean result1 = check1.executeCheck(oneTableData);
            assertTrue("Expected one table dataset to pass.", result1);

        } catch (Exception e) {
            e.printStackTrace();
            fail("Test threw an exception: " + e.getMessage());
        }
    }
    
    @Test
    public void singleTableCheckWrongTest()
    {
    	try {
            DatasetProfile profile2 = facade.getProfile("frame4");
            Dataset<Row> twoTablesData = profile2.getDataset();
            
            CsvSingleTableCheck check2 = new CsvSingleTableCheck();
            boolean result2 = check2.executeCheck(twoTablesData);
            assertFalse("Expected two tables dataset to fail.", result2);

        } catch (Exception e) {
            e.printStackTrace();
            fail("Test threw an exception: " + e.getMessage());
        }
    }
    
    @Test
    public void titleInDistributionCheckTest()
    {
    	try {
            DatasetProfile profile1 = facade.getProfile("frame3");
            Dataset<Row> noTitleData = profile1.getDataset();

            CsvTitleInDistributionCheck check1 = new CsvTitleInDistributionCheck();
            boolean result1 = check1.executeCheck(noTitleData);
            assertTrue("Expected no title inside this dataset.", result1);

        } catch (Exception e) {
            e.printStackTrace();
            fail("Test threw an exception: " + e.getMessage());
        }
    }
    
    @Test
    public void titleInDistributionCheckWrongTest()
    {
    	try {
            DatasetProfile profile2 = facade.getProfile("frame6");
            Dataset<Row> titleInsideData = profile2.getDataset();
            
            CsvTitleInDistributionCheck check2 = new CsvTitleInDistributionCheck();
            boolean result2 = check2.executeCheck(titleInsideData);
            assertFalse("Expected title inside this dataset.", result2);
            
        } catch (Exception e) {
            e.printStackTrace();
            fail("Test threw an exception: " + e.getMessage());
        }
    }
    
    @Test
    public void uniformColumnCountCheckTest()
    {
    	try {
            DatasetProfile profile1 = facade.getProfile("frame3");
            Dataset<Row> oneTableData = profile1.getDataset();

            CsvUniformColumnCountCheck check1 = new CsvUniformColumnCountCheck();
            boolean result1 = check1.executeCheck(oneTableData);
            assertTrue("Expected equal number of rows and columns.", result1);

        } catch (Exception e) {
            e.printStackTrace();
            fail("Test threw an exception: " + e.getMessage());
        }
    }
    
    @Test
    public void uniformColumnCountCheckWrongTest()
    {
    	try {
            DatasetProfile profile2 = facade.getProfile("frame5");
            Dataset<Row> twoTablesData = profile2.getDataset();
            
            CsvUniformColumnCountCheck check2 = new CsvUniformColumnCountCheck();
            boolean result2 = check2.executeCheck(twoTablesData);
            assertFalse("Expected more columns in some rows.", result2);

        } catch (Exception e) {
            e.printStackTrace();
            fail("Test threw an exception: " + e.getMessage());
        }
    }
    
    /*    @Test
    public void dataAccessRestrictionCheckTest()
    {
    	try {
            //Accessible Dataset
            DatasetProfile profile1 = facade.getProfile("frame3");
            Dataset<Row> accessibleData = profile1.getDataset();

            DataAccessRestrictionCheck check1 = new DataAccessRestrictionCheck();
            boolean result1 = check1.executeCheck(accessibleData);
            assertTrue("Expected accessible dataset to pass.", result1);


            //Restricted Dataset
            DatasetProfile profile2 = facade.getProfile("frame9");
            if (profile2 == null) {
                System.out.println("Restricted dataset could not be loaded — possibly due to access denial.");
                assertTrue("File is not accessible, restriction check should fail.", true);
                return;
            }

            Dataset<Row> restrictedData = profile2.getDataset();
            DataAccessRestrictionCheck check2 = new DataAccessRestrictionCheck();
            boolean result2 = check2.executeCheck(restrictedData);
            assertFalse("Expected restricted dataset to fail.", result2);

        } catch (Exception e) {
            e.printStackTrace();
            fail("Test threw an exception: " + e.getMessage());
        }
    }*/
    
    @Test
    public void duplicateDataCheckTest()
    {
    	try 
    	{
    		DatasetProfile profile = facade.getProfile("frame1");
        	
            Dataset<Row> dataset = profile.getDataset();
            
        	DuplicateDataCheck check = new DuplicateDataCheck();
        	boolean validResult = check.executeCheck(dataset);
            assertTrue("Expected correct dataset to pass the duplicate data check.", validResult);
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    	
    }
    
    @Test
    public void duplicateDataCheckWrongTest()
    {
    	try 
    	{
    		DatasetProfile profile = facade.getProfile("frame2");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            DuplicateDataCheck check = new DuplicateDataCheck();
        	boolean result = check.executeCheck(dataset);
        	assertFalse("The dataset has a duplicate row", result);
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    }
    
    @Test
    public void separateSheetDatasetCheckTest()
    {
    	try {
            DatasetProfile profile1 = facade.getProfile("frame7");
            Dataset<Row> oneSheetDataset = profile1.getDataset();

            SeparateSheetDatasetCheck check1 = new SeparateSheetDatasetCheck();
            boolean result1 = check1.executeCheck(oneSheetDataset);
            assertTrue("Expected accessible dataset to pass.", result1);

        } catch (Exception e) {
            e.printStackTrace();
            fail("Test threw an exception: " + e.getMessage());
        }
    }  
    
    @Test
    public void separateSheetDatasetCheckWrongTest()
    {
    	try {
            DatasetProfile profile2 = facade.getProfile("frame8");
            Dataset<Row> twoSheetsDataset = profile2.getDataset();
            
            SeparateSheetDatasetCheck check2 = new SeparateSheetDatasetCheck();
            boolean result2 = check2.executeCheck(twoSheetsDataset);
            assertFalse("Expected two sheet dataset to fail.", result2);

        } catch (Exception e) {
            e.printStackTrace();
            fail("Test threw an exception: " + e.getMessage());
        }
    }  
    
    @Test
    public void sufficientDataCheckTest()
    {
    	try 
    	{
    		DatasetProfile profile = facade.getProfile("frame1");
        	
            Dataset<Row> dataset = profile.getDataset();
            
        	SufficientDataCheck check = new SufficientDataCheck();
        	boolean validResult = check.executeCheck(dataset);
            assertTrue("Expected correct dataset to pass the sufficient data check.", validResult);
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    	
    }
    
    @Test
    public void sufficientDataCheckWrongTest()
    {
    	try 
    	{
    		DatasetProfile profile = facade.getProfile("frame2");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            SufficientDataCheck check = new SufficientDataCheck();
        	boolean result = check.executeCheck(dataset);
        	assertFalse("The dataset has not enough data", result);
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    }
    
    @Test
    public void uniqueIdentifierCheckTest()
    {
    	try 
    	{
    		DatasetProfile profile = facade.getProfile("frame3");
        	
            Dataset<Row> dataset = profile.getDataset();
            
        	UniqueIdentifierCheck check = new UniqueIdentifierCheck();
        	boolean validResult = check.executeCheck(dataset);
            assertTrue("Expected correct dataset to pass the unique URI check.", validResult);
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    	
    }
    
    @Test
    public void uniqueIdentifierCheckWrongTest()
    {
    	try 
    	{
    		DatasetProfile profile = facade.getProfile("frame4");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            UniqueIdentifierCheck check = new UniqueIdentifierCheck();
        	boolean result = check.executeCheck(dataset);
        	assertFalse("The dataset hasn't valid URI column", result);
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    }
    
    @Test
    public void uriDetailsRetrievalCheckTest()
    {
    	try 
    	{
    		DatasetProfile profile = facade.getProfile("frame3");
        	
            Dataset<Row> dataset = profile.getDataset();
            
        	UriDetailsRetrievalCheck check = new UriDetailsRetrievalCheck();
        	boolean validResult = check.executeCheck(dataset);
            assertTrue("Expected correct dataset to pass the URI details retrieved check.", validResult);
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    	
    }
    
    @Test
    public void uriDetailsRetrievalCheckWrongTest()
    {
    	try 
    	{
    		DatasetProfile profile = facade.getProfile("frame5");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            UriDetailsRetrievalCheck check = new UriDetailsRetrievalCheck();
        	boolean result = check.executeCheck(dataset);
        	assertFalse("The dataset hasn't a label column for the URI column", result);
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    }
    
    @Test
    public void uriLinkedDataCheckTest()
    {
    	try 
    	{
    		DatasetProfile profile = facade.getProfile("frame3");
        	
            Dataset<Row> dataset = profile.getDataset();
            
        	UriLinkedDataCheck check = new UriLinkedDataCheck();
        	boolean validResult = check.executeCheck(dataset);
            assertTrue("Expected correct dataset to pass the URI Linked Data test.", validResult);
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    	
    }
    
    @Test
    public void uriLinkedDataCheckWrongTest()
    {
    	try 
    	{
    		DatasetProfile profile = facade.getProfile("frame5");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            UriLinkedDataCheck check = new UriLinkedDataCheck();
        	boolean result = check.executeCheck(dataset);
        	assertFalse("The dataset can't be published as Linked Data", result);
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    }
      
    @Test
    public void utf8EncodingCheckTest()
    {
    	try 
    	{
    		DatasetProfile profile = facade.getProfile("frame3");
        	
            Dataset<Row> dataset = profile.getDataset();
            
        	Utf8EncodingCheck check = new Utf8EncodingCheck();
        	boolean validResult = check.executeCheck(dataset);
            assertTrue("Expected correct dataset to pass the UTF-8 encoding check.", validResult);
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    	
    }
    
    @Test
    public void utf8EncodingCheckWrongTest()
    {
    	try 
    	{
    		DatasetProfile profile = facade.getProfile("frame4");
        	
            Dataset<Row> dataset = profile.getDataset();
            
            Utf8EncodingCheck check = new Utf8EncodingCheck();
        	boolean result = check.executeCheck(dataset);
        	assertFalse("The dataset hasn't UTF-8 encoding", result);
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    }
    
    @Test
    public void unitInDedicatedColumnTest()
    {
    	try 
    	{
    		DatasetProfile profile = facade.getProfile("frame1");
        	
            Dataset<Row> dataset = profile.getDataset();
            
        	CsvUnitInDedicatedColumnCheck check = new CsvUnitInDedicatedColumnCheck();
        	boolean validResult = check.executeCheck(dataset);
            assertTrue("Expected correct dataset to pass the unit-in-column check.", validResult);
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    	
    }
    
    @Test
    public void unitInDedicatedColumnWrongTest()
    {
    	try 
    	{
    		DatasetProfile profile = facade.getProfile("frame2");
        	
            Dataset<Row> dataset = profile.getDataset();
            
        	CsvUnitInDedicatedColumnCheck check = new CsvUnitInDedicatedColumnCheck();
        	boolean result = check.executeCheck(dataset);
        	assertFalse("Expected multiple units in a dedicated column.", result);
    	}
    	catch (Exception e)
        {
            System.out.println(e);
            assertTrue(false);
        }
    }

}
