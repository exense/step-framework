package step.core.collections.inmemory;

import org.junit.Assert;
import org.junit.Test;
import step.core.collections.AbstractCollectionTest;
import step.core.collections.Document;

import java.util.Properties;

public class InMemoryCollectionTest extends AbstractCollectionTest {

    public InMemoryCollectionTest() {
        super(new InMemoryCollectionFactory(new Properties()));
    }

    @Test
    public void testDrop() {
        InMemoryCollection<Document> c = new InMemoryCollection<>();
        c.save(new Document());
        c.save(new Document());
        Assert.assertEquals(2, c.estimatedCount());
        c.drop();
        Assert.assertEquals(0, c.estimatedCount());
    }
}
