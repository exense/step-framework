package step.core.collections.inmemory;

import java.util.Properties;

import step.core.collections.AbstractCollectionTest;

public class InMemoryCollectionTest extends AbstractCollectionTest {

	public InMemoryCollectionTest() {
		super(new InMemoryCollectionFactory(new Properties()));
	}

}
