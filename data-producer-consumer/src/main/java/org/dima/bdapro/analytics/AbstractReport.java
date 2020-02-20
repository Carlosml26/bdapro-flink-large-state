package org.dima.bdapro.analytics;

import org.dima.bdapro.datalayer.bean.TransactionWrapper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public abstract class AbstractReport implements Report {

	protected BufferedWriter outputFileWriter;
	protected BufferedWriter statsFileWrtier;


	@Override
	public void process(TransactionWrapper wrapper) {
		processRecord(wrapper);
	}

	@Override
	public void materialize() throws IOException {
		outputResults();
		reset();
	}

	public void reset() {
		try {
			outputFileWriter.flush();
			statsFileWrtier.flush();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected void initStatsFile(String statsFileName) throws IOException {
		statsFileWrtier = createWriter(statsFileName);
	}

	protected void initOutputFile(String outputFileName) throws IOException {
		outputFileWriter = createWriter(outputFileName);
	}

	private BufferedWriter createWriter(String fileName) throws IOException {
		File file = new File(fileName);
		if (file.exists()) {
			file.delete();
			file.createNewFile();
		}
		return new BufferedWriter(new FileWriter(file));
	}

	@Override
	public void close() throws IOException {
		outputFileWriter.close();
		statsFileWrtier.close();
	}
}
