package fairchecks.api;

import java.util.List;

import org.apache.spark.sql.types.DataType;

/**
 * An extension of {@link IGenericCheck} designed specifically for column-level checks.
 *
 * <p>Column checks evaluate individual columns in a dataset and may provide detailed
 * feedback on invalid entries as well as applicability based on data type.
 * 
 * <p><strong>Important:</strong> Before executing a column check, {@link #isApplicable(DataType)} must be called to
 * determine whether the check is relevant for the specific column type. Only applicable checks should be executed.
 *
 */
public interface IGenericColumnCheck extends IGenericCheck{
	
	/**
	 * Returns a list of messages describing which rows failed the check and why.
	 * 
     * @return a list of invalid row descriptions
     */
	public List<String> getInvalidRows();
	
	/**
	 * Determines whether this check is applicable to the given column data type.
	 * 
	 * <p>This method should be called before executing the check on a column.
	 * 
     * @param columnType the type of the column
     * @return {@code true} if the check can be applied, {@code false} otherwise
     */
	public boolean isApplicable(DataType columnType);
}