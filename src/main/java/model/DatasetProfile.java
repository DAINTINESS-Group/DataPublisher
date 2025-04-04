package model;

import java.io.Serializable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

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
