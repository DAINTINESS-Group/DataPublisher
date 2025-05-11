package report;

import utils.ReportType;

/**
 * Factory class for creating instances of {@link IChecksReportGenerator} based on a specified {@link ReportType}.
 *
 */
public class ChecksReportGeneratorFactory {
	
	public IChecksReportGenerator createReportGenerator(ReportType type) {
		switch (type) {
			case TEXT:
				return new TxtChecksReportGenerator();
			case MARKDOWN:
				return new MarkdownChecksReportGenerator();
			case JSON:
				return null;
			default:
				throw new IllegalArgumentException("Unsupported report type: " + type);
		}
	}
}
