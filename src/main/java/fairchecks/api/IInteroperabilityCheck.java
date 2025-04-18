package fairchecks.api;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;

/**
 * Marker interface for all FAIR data quality checks that belong to the "Interoperability" category.
 * <p>
 * Implementing classes should define the logic required to verify whether a dataset or column 
 * satisfies specific interoperability criteria.
 * </p>
 *
 * @see fairchecks.factory
 * @see fairchecks.checks
 */
public interface IInteroperabilityCheck {
	String getCheckId();
	String getCheckDescription();
    boolean executeCheck(Dataset<Row> dataset);
    default List<String> getInvalidRows() { return null; }
    default boolean isApplicable(DataType columnType) { return true; }
}
