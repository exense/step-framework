/*******************************************************************************
 * Copyright (C) 2020, exense GmbH
 *  
 * This file is part of STEP
 *  
 * STEP is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *  
 * STEP is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *  
 * You should have received a copy of the GNU Affero General Public License
 * along with STEP.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package step.core.collections.filesystem;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import step.core.collections.Collection;
import step.core.collections.CollectionFactory;
import step.core.collections.VersionableEntity;

public class FilesystemCollectionFactory implements CollectionFactory {

	public static final String FILESYSTEM_PATH = "path";
	private final File workspace;
	
	public FilesystemCollectionFactory(Properties properties) {
		super();
		this.workspace = new File(properties.getProperty(FILESYSTEM_PATH, "db"));
	}

	public FilesystemCollectionFactory(File workspace) {
		super();
		this.workspace = workspace;
	}

	@Override
	public void close() throws IOException {
		
	}

	@Override
	public <T> Collection<T> getCollection(String name, Class<T> entityClass) {
		return new FilesystemCollection<>(new File(workspace.getAbsolutePath()+"/"+name), entityClass);
	}

	@Override
	public Collection<VersionableEntity> getVersionedCollection(String name) {
		return new FilesystemCollection<>(
				new File(workspace.getAbsolutePath() + "/" + name + CollectionFactory.VERSION_COLLECTION_SUFFIX),
				VersionableEntity.class);
	}

}
