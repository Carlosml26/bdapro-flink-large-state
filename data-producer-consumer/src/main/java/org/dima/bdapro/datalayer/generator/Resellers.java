package org.dima.bdapro.datalayer.generator;

import org.dima.bdapro.utils.PropertiesHandler;

import java.io.IOException;
import java.util.Properties;

public class Resellers {

	private String threadId;
	private String[][] resellers;
	private String[] resellerTypes;

	public Resellers(String threadId) throws IOException {
		this.threadId = threadId;

		init();
	}

	private void init() throws IOException {
		Properties props = PropertiesHandler.getInstance().getModuleProperties();
		resellerTypes = props.getProperty("datagenerator.resellers.types", "operator,reseller").split(",");
		String format = props.getProperty("datagenerator.resellers.format");

		resellers = new String[resellerTypes.length][];

		for (int i = 0; i < resellerTypes.length; i++) {
			String type = resellerTypes[i];


			String prefix = props.getProperty("datagenerator.resellers." + type);
			int count = Integer.parseInt(props.getProperty("datagenerator.resellers." + type + ".count", "1"));
			String[] arr = new String[count];

			for (int j = 0; j < count; j++) {
				arr[j] = String.format(threadId, prefix, count);
			}
			resellers[i] = arr;
		}

	}

	public int getTypesSize() {
		return resellers.length;
	}

	public String getTypeName(int type) {
		return resellerTypes[type];
	}

	public String getResellerId(int type, int id) {
		return resellers[type][id % resellers[type].length];
	}


}
