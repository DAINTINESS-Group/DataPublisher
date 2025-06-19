package report;

import org.junit.Test;
import utils.ReportType;

import static org.junit.Assert.*;

public class ChecksReportGeneratorFactoryTest {
	
	private final ChecksReportGeneratorFactory factory = new ChecksReportGeneratorFactory();
	
	@Test
	public void testCreateReportGeneratorTextType() {
		IChecksReportGenerator generator = factory.createReportGenerator(ReportType.TEXT);
		
		assertNotNull(generator);
		assertEquals(TxtChecksReportGenerator.class, generator.getClass());
	}
	
	@Test
	public void testCreateReportGeneratorMarkdownType() {
		IChecksReportGenerator generator = factory.createReportGenerator(ReportType.MARKDOWN);
		
		assertNotNull(generator);
		assertEquals(MarkdownChecksReportGenerator.class, generator.getClass());
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testCreateReportGeneratorUnsupportedType() {
		factory.createReportGenerator(ReportType.JSON);
	}
}
