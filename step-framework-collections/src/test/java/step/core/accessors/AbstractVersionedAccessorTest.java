package step.core.accessors;

import org.bson.types.ObjectId;
import org.junit.Test;
import step.core.collections.EntityVersion;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class AbstractVersionedAccessorTest extends AbstractAccessorTest{

	public AbstractVersionedAccessorTest() {
		super();
	}

	@Test
	public void testBeanAccessorHistory() throws InterruptedException {
		Bean entity = new Bean();
		ObjectId id = entity.getId(); //required for inMemory collections tests or only the last value remain (could it be a bug?)
		entity.setProperty1("value 1");
		beanAccessor.save(entity);

		Thread.sleep(2); // version threshold is 1 ms

		entity.setProperty1("value 2");
		beanAccessor.save(entity);

		Thread.sleep(2); // version threshold is 1 ms

		entity.setProperty1("value 3");
		beanAccessor.save(entity);
		assertEquals("value 3", beanAccessor.get(entity.getId()).getProperty1());

		List<EntityVersion> history = beanAccessor.getHistory(entity.getId(), null, null).collect(Collectors.toList());
		assertEquals(3, history.size());
		int i = 3;
		for (EntityVersion v : history) {
			assertEquals("value " + i, ((Bean) v.getEntity()).getProperty1());
			assertEquals(v.getId().toHexString(),((Bean) v.getEntity()).getCustomField(EntityVersion.VERSION_CUSTOM_FIELD).toString());
			i--;
		}
		beanAccessor.remove(entity.getId());
		assertEquals(null, beanAccessor.get(entity.getId()));
		assertEquals(0,
				beanAccessor.getHistory(entity.getId(), null, null).collect(Collectors.toList()).size());
	}

	@Test
	public void testBeanAccessorBulkHistory() throws InterruptedException {
		Bean entity = new Bean();
		entity.setProperty1("value 1");
		Bean entity1 = new Bean();
		entity1.setProperty1("value 11");
		beanAccessor.save(List.of(entity, entity1));

		Thread.sleep(2); // version threshold is 1 ms

		entity.setProperty1("value 2");
		entity1.setProperty1("value 12");
		beanAccessor.save(List.of(entity, entity1));

		assertEquals("value 2", beanAccessor.get(entity.getId()).getProperty1());
		assertEquals("value 12", beanAccessor.get(entity1.getId()).getProperty1());

		List<EntityVersion> history = beanAccessor.getHistory(entity.getId(), null, null).collect(Collectors.toList());
		assertEquals(2, history.size());
		int i = 2;
		for (EntityVersion v : history) {
			assertEquals("value " + i, ((Bean) v.getEntity()).getProperty1());
			assertEquals(v.getId().toHexString(),((Bean) v.getEntity()).getCustomField(EntityVersion.VERSION_CUSTOM_FIELD).toString());
			i--;
		}

		List<EntityVersion> history1 = beanAccessor.getHistory(entity1.getId(), null, null).collect(Collectors.toList());
		assertEquals(2, history1.size());
		i = 2;
		for (EntityVersion v : history1) {
			assertEquals("value 1" + i, ((Bean) v.getEntity()).getProperty1());
			assertEquals(v.getId().toHexString(),((Bean) v.getEntity()).getCustomField(EntityVersion.VERSION_CUSTOM_FIELD).toString());
			i--;
		}
	}

	@Test
	public void testBeanAccessorRestoreHistory() throws InterruptedException {
		Bean entity = new Bean();
		entity.setProperty1("value 1");
		beanAccessor.save(entity);

		Thread.sleep(2l);

		entity.setProperty1("value 2");
		beanAccessor.save(entity);

		Thread.sleep(2l);

		entity.setProperty1("value 3");
		beanAccessor.save(entity);
		assertEquals("value 3", beanAccessor.get(entity.getId()).getProperty1());

		List<EntityVersion> history = beanAccessor.getHistory(entity.getId(), null, null).collect(Collectors.toList());
		assertEquals(3, history.size());

		Bean bean = beanAccessor.restoreVersion(entity.getId(), history.get(1).getId());
		assertEquals("value 2", bean.getProperty1());
		assertEquals("value 2", beanAccessor.get(entity.getId()).getProperty1());
		assertEquals(history.get(1).getId().toHexString(),bean.getCustomField(EntityVersion.VERSION_CUSTOM_FIELD).toString());

	}


 
}