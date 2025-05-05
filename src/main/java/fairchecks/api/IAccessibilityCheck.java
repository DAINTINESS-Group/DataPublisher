package fairchecks.api;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

/**
 * Marker interface for all FAIR data quality checks that belong to the "Accessibility" category.
 * <p>
 * Implementing classes should define the logic required to verify whether a dataset or column 
 * satisfies specific accessibility criteria.
 * </p>
 *
 * @see fairchecks.factory
 * @see fairchecks.checks
 */
public interface IAccessibilityCheck {
	public String getCheckId();
	public String getCheckDescription();
    public boolean executeCheck(Dataset<Row> dataset);
    public default List<String> getInvalidRows() { return null; }
    public default boolean isApplicable(DataType columnType) { return true; }
}