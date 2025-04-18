package config;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

/**
 * Provides configuration and access to the Apache Spark environment used throughout the application.
 * <p>
 * This class creates a local SparkSession instance with logging disabled,
 * which serves as the entry point for interacting with Spark's SQL API.
 * It is designed to be shared across components that need access to the dataset processing engine.
 * </p>
 */
public class SparkConfig {
	
	private String appName = "Java Spark SQL";
    private String appKey = "spark.master";
    private String appValue = "local";

    private SparkSession spark;

    public SparkConfig()
    {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        
        spark = SparkSession
                        .builder()
                        .appName("Java Spark SQL")
                        .master("local[*]")
                        .config("spark.master", "local")
                        .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");
    }

    public SparkSession getSparkSession()
    {
        return spark;
    }

    public String getAppName() {
        return appName;
    }

    public String getAppKey() {
        return appKey;
    }

    public String getAppValue() {
        return appValue;
    }

}
