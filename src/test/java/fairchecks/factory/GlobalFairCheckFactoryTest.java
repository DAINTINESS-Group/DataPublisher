package fairchecks.factory;

import fairchecks.api.IGenericCheck;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class GlobalFairCheckFactoryTest {
	
	@Test
	public void testGetAllGlobalChecks() {
		List<IGenericCheck> allChecks = GlobalFairCheckFactory.getAllGlobalChecks();
		
		assertEquals(16, allChecks.size());
		
		assertTrue(containsCheck(allChecks, "Utf8EncodingCheck"));
		assertTrue(containsCheck(allChecks, "UniqueIdentifierCheck"));
		assertTrue(containsCheck(allChecks, "SufficientDataCheck"));
	}
	
	@Test
	public void testGetFindabilityChecks() {
		List<IGenericCheck> findabilityChecks = GlobalFairCheckFactory.getFindabilityChecks();
		
		assertTrue(findabilityChecks.isEmpty());
	}
	
	@Test
	public void testGetAccessibilityChecks() {
		List<IGenericCheck> accessibilityChecks = GlobalFairCheckFactory.getAccessibilityChecks();
		
		assertEquals(1, accessibilityChecks.size());
		assertEquals("DataAccessRestrictionCheck", accessibilityChecks.get(0).getClass().getSimpleName());
	}
	
	@Test
	public void testGetInteroperabilityChecks() {
		List<IGenericCheck> interoperabilityChecks = GlobalFairCheckFactory.getInteroperabilityChecks();
		
		assertEquals(12, interoperabilityChecks.size());
		
		assertTrue(containsCheck(interoperabilityChecks, "Utf8EncodingCheck"));
		assertTrue(containsCheck(interoperabilityChecks, "UriLinkedDataCheck"));
		assertTrue(containsCheck(interoperabilityChecks, "CsvUnitInDedicatedColumnCheck"));
	}
	
	@Test
	public void testGetReusabilityChecks() {
		List<IGenericCheck> reusabilityChecks = GlobalFairCheckFactory.getReusabilityChecks();
		
		assertEquals(3, reusabilityChecks.size());
		
		assertTrue(containsCheck(reusabilityChecks, "SufficientDataCheck"));
		assertTrue(containsCheck(reusabilityChecks, "DuplicateDataCheck"));
		assertTrue(containsCheck(reusabilityChecks, "SeparateSheetDatasetCheck"));
	}
	
	private boolean containsCheck(List<IGenericCheck> list, String className) {
		return list.stream().anyMatch(c -> c.getClass().getSimpleName().equals(className));
	}

}
