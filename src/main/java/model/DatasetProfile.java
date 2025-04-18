package model;

import java.io.Serializable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * This class represents a Dataset, from a CSV file, as well as information about it, as well as
 * any executed requests and their results.
 * 
 * @param alias A string, unique amongst the registered DatasetProfiles.
 * @param filePath The path of the represented Dataset.
 * @param fileHasHeader A boolean that represents whether the file has a header. Used in report
 * generation for accurate line counting.
 * @param dataset A Spark <code>Dataset<Row></code>. Essentially the collection of rows of data from a CSV.
 *
 * @see Dataset
 */
public class DatasetProfile implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private String alias;
	private String filePath;
	private boolean fileHasHeader;
	private Dataset<Row> dataset;
	
	public DatasetProfile(String alias, Dataset<Row> dataset, String path, boolean fileHasHeader) {
		this.alias = alias;
		this.filePath = path;
		this.dataset = dataset;
		this.fileHasHeader = fileHasHeader;
	}
	
	public String getAlias() {
		return alias;
	}
	
	public String getFilePath() {
		return filePath;
	}
	
	public Dataset<Row> getDataset(){
		return dataset;
	}
	
	public boolean hasFileHeader() {
		return fileHasHeader;
	}

}
