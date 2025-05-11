package engine;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;

import config.SparkConfig;
import model.DatasetProfile;
import utils.RegistrationResponse;

/**
 * Responsible for registering datasets (from CSV files) into the system and storing them as {@link DatasetProfile} instances.
 * <p>
 * This controller manages Spark-based dataset loading, ensures unique aliases,
 * and exposes methods to retrieve or list registered dataset profiles.
 * A <code>RegistrationResponse</code> is returned to represent if the registration
 * was successful.
 * </p>
 * 
 * @see DatasetProfile
 * @see RegistrationResponse
 */
public class DatasetRegistrationController {
	
	private ArrayList<DatasetProfile> datasetProfiles = new ArrayList<DatasetProfile>();
	private SparkSession spark;
	
	public DatasetRegistrationController() {
		spark = new SparkConfig().getSparkSession();
	}
	
	public RegistrationResponse registerDataset(String path, String alias, boolean hasHeader) {
		String separator = detectSeparator(path);
		try 
		{
			if(existsProfileWithAlias(alias)) return RegistrationResponse.ALIAS_EXISTS;

			Dataset<Row> df = spark.read().option("header", hasHeader)
					.option("inferSchema", true)
					.option("delimiter", separator)
					.csv(path)
					.withColumn("_id", functions.row_number().over(Window.orderBy(functions.lit("A"))));

			df.sparkSession().conf().set("spark.sql.csv.filepath", path);
			df.sparkSession().conf().set("dataset.separator", separator);
			
			DatasetProfile profile = new DatasetProfile(alias, df, path, hasHeader);
			datasetProfiles.add(profile);
			
			return RegistrationResponse.SUCCESS;
		}
		catch(Exception e) 
		{
			return RegistrationResponse.FAILURE;
		}
	}
	
	private boolean existsProfileWithAlias(String alias) {
		for (DatasetProfile profile: datasetProfiles) {
			if(profile.getAlias().equals(alias)) return true;
		}
		return false;
	}
	
	private static String detectSeparator(String encodedPath) {
        try {
        	File file;
            if (encodedPath.startsWith("file:/")) {
                URI uri = new URI(encodedPath);
                file = new File(uri);
            } else {
                file = new File(encodedPath); // Directly use normal file path
            }
            int semicolonCount = 0;
            int commaCount = 0;

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
                String line;
                int linesChecked = 0;

                while ((line = reader.readLine()) != null && linesChecked < 10) {
                    semicolonCount += countOccurrences(line, ';');
                    commaCount += countOccurrences(line, ',');
                    linesChecked++;
                }
            }

            //System.out.println("Detected counts - Semicolons: " + semicolonCount + ", Commas: " + commaCount);

            if (semicolonCount >= commaCount) {
                return ";";
            } else {
                return ",";
            }

        } catch (Exception e) {
            e.printStackTrace();
            return ",";
        }
    }

    private static int countOccurrences(String line, char separator) {
        int count = 0;
        for (char c : line.toCharArray()) {
            if (c == separator) count++;
        }
        return count;
    }
	
	public ArrayList<DatasetProfile> getProfiles(){
		return datasetProfiles;
	}
	
	public DatasetProfile getProfile(String alias) {
		for (DatasetProfile profile: datasetProfiles) {
			if (profile.getAlias().equals(alias)) return profile;
		}
		return null;
	}

}
