package fairchecks.factory;

import fairchecks.api.IGenericCheck;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class ColumnFairCheckFactoryTest {
	
	private static final String COLUMN_NAME = "testColumn";
	
	@Test
	public void testGetAllColumnChecks() {
		List<IGenericCheck> checks = ColumnFairCheckFactory.getAllColumnChecks(COLUMN_NAME);
		
		assertEquals(8, checks.size());
		
		assertTrue(containsCheck(checks, "NullValueMarkingCheck"));
		assertTrue(containsCheck(checks, "DateTimeFormatCheck"));
		assertTrue(containsCheck(checks, "DataAccuracyCheck"));
	}
	
	@Test
	public void testGetFindabilityChecks() {
		List<IGenericCheck> checks = ColumnFairCheckFactory.getFindabilityChecks(COLUMN_NAME);
		
		assertEquals(1, checks.size());
		assertEquals("NullValueMarkingCheck", checks.get(0).getClass().getSimpleName());
	}
	
	@Test
	public void testGetAccessibilityChecks() {
		List<IGenericCheck> checks = ColumnFairCheckFactory.getAccessibilityChecks(COLUMN_NAME);
		
		assertTrue(checks.isEmpty());
	}
	
	@Test
	public void testGetInteroperabilityChecks() {
		List<IGenericCheck> checks = ColumnFairCheckFactory.getInteroperabilityChecks(COLUMN_NAME);
		
		assertEquals(6, checks.size());
		
		assertTrue(containsCheck(checks, "DateTimeFormatCheck"));
		assertTrue(containsCheck(checks, "ControlledVocabularyCheck"));
		assertTrue(containsCheck(checks, "CsvUnitInHeaderCheck"));
	}
	
	@Test
	public void testGetReusabilityChecks() {
		List<IGenericCheck> checks = ColumnFairCheckFactory.getReusabilityChecks(COLUMN_NAME);
		
		assertEquals(1, checks.size());
		assertEquals("DataAccuracyCheck", checks.get(0).getClass().getSimpleName());
	}
	
	private boolean containsCheck(List<IGenericCheck> list, String className) {
		return list.stream().anyMatch(c -> c.getClass().getSimpleName().equals(className));
	}

}
