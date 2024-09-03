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
package step.core.accessors;

import ch.exense.commons.io.FileHelper;
import org.junit.Before;
import step.core.collections.EntityVersion;
import step.core.collections.filesystem.FilesystemCollection;

import java.io.File;
import java.io.IOException;

public class FilesystemVersionedAccessorTest extends AbstractVersionedAccessorTest {

	@Before
	public void before() {
		File repository;
		try {
			repository = FileHelper.createTempFolder();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		accessor = new AbstractAccessor<AbstractIdentifiableObject>(
				new FilesystemCollection<>(repository, AbstractIdentifiableObject.class));
		organizableObjectAccessor = new AbstractAccessor<AbstractOrganizableObject>(
				new FilesystemCollection<>(repository, AbstractOrganizableObject.class));
		beanAccessor = new AbstractAccessor<Bean>(
				new FilesystemCollection<>(repository, Bean.class));
		beanAccessor.enableVersioning(new FilesystemCollection<>(repository, EntityVersion.class), 1l);
		pseudoBeanAccessor = new AbstractAccessor<>(
				new FilesystemCollection<>(repository, PseudoBean.class));
		pseudoBeanAccessor.enableVersioning(new FilesystemCollection<>(repository, EntityVersion.class), 1l);
	}
}
