package utils;

//import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.INIConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;

import java.io.File;
import java.util.*;

public class IniFairCheckLoader {
	
	public static Map<String, String> loadFairChecks(String filePath){
		Map<String, String> fairChecks = new LinkedHashMap<>();
		
		try {
			Configurations configs = new Configurations();
			INIConfiguration ini = configs.ini(new File(filePath));
			
			Iterator<String> keys = ini.getKeys("GLOBAL");
			
			while(keys.hasNext()) {
				String key = keys.next();
				fairChecks.put(key.replace("GLOBAL.", ""), ini.getString(key));
			}
		} catch (Exception e) {
			System.err.println("Error loading INI file: " + e.getMessage());
		}
		
		return fairChecks;
	}

}
