package fairchecks.api;

import java.util.List;

import org.apache.spark.sql.types.DataType;

/**
 * An extension of {@link IGenericCheck} designed specifically for column-level checks.
 *
 * <p>Column checks evaluate individual columns in a dataset and may provide detailed
 * feedback on invalid entries as well as applicability based on data type.
 *
 * <p>This allows checks to be selectively applied based on schema and return targeted results.
 */
public interface IGenericColumnCheck extends IGenericCheck{
	
	/**
     * @return a list of invalid row descriptions
     */
	public List<String> getInvalidRows();
	
	/**
     * Determines whether this check is applicable to the given column data type.
     *
     * @param columnType
     * @return {@code true} if the check can be applied, {@code false} otherwise
     */
	public boolean isApplicable(DataType columnType);
}