package step.core.collections.inmemory;

import step.core.collections.AbstractCollectionTest;

import java.util.Properties;

public class InMemoryCollectionTest extends AbstractCollectionTest {

    public InMemoryCollectionTest() {
        super(new InMemoryCollectionFactory(new Properties()));
    }

    @Override
    public void renameCollections() {
        //not implemented for in memory
    }
}
