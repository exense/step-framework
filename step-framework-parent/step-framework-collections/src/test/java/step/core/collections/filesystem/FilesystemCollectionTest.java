package step.core.collections.filesystem;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import ch.exense.commons.io.FileHelper;
import step.core.collections.AbstractCollectionTest;

public class FilesystemCollectionTest extends AbstractCollectionTest {

	public FilesystemCollectionTest() throws IOException {
		super(new FilesystemCollectionFactory(getProperties()));
	}
	
	private static Properties getProperties() throws IOException {
		File folder = FileHelper.createTempFolder();
		Properties properties = new Properties();
		properties.put(FilesystemCollectionFactory.FILESYSTEM_PATH, folder.getAbsolutePath());
		return properties;
	}
}
