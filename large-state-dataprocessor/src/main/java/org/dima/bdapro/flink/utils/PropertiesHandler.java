package org.dima.bdapro.flink.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesHandler
{
	private static PropertiesHandler propertiesHandler;

	private Properties moduleProperties;


	private PropertiesHandler(String filePath) throws IOException
	{
		loadFromFile(filePath);
	}


	/**
	 * Return the singleton instance of for class {@link PropertiesHandler} using property
	 *
	 * @param filePath
	 * @return
	 */
	public static PropertiesHandler getInstance(String... filePath) throws IOException
	{
		synchronized (PropertiesHandler.class)
		{
			if (propertiesHandler == null)
			{
				propertiesHandler = new PropertiesHandler(filePath[0]);
			}
		}
		return propertiesHandler;
	}

	public Properties getModuleProperties()
	{
		return moduleProperties;
	}

	public void setModuleProperties(Properties sysProperties)
	{
		moduleProperties = sysProperties;
	}

	public String getProperty(String key)
	{
		return moduleProperties.getProperty(key);
	}

	public void loadFromFile(String filePath) throws IOException
	{

		if (null == filePath)
		{
			InputStream is = getClass().getResourceAsStream("/module-config.properties");
			if (is == null)
			{
				throw new FileNotFoundException("Could not read properties file from classpath: module-config.properties");
			}

			moduleProperties = new Properties();
			moduleProperties.load(is);
		}
		else
		{
			File propertiesFile;
			propertiesFile = new File(filePath);
			if (!(propertiesFile.exists() && propertiesFile.canRead()))
			{
				throw new FileNotFoundException("Unable to read file : " + propertiesFile.getPath());
			}

			moduleProperties = new Properties();
			moduleProperties.load(new FileInputStream(propertiesFile));
		}

	}


}
