package step.core.accessors;

import org.junit.Before;
import org.junit.Test;
import step.core.collections.inmemory.InMemoryCollection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CachedVersionedAccessorTest extends AbstractVersionedAccessorTest {

	private InMemoryAccessor<AbstractIdentifiableObject> underlyingAccessor = new InMemoryAccessor<>(false);
	private InMemoryAccessor<AbstractOrganizableObject> underlyingOrganisableObjectAccessor = new InMemoryAccessor<>(false);
	private InMemoryAccessor<Bean> underlyingBeanAccessor = new InMemoryAccessor<>(false);
	private InMemoryAccessor<PseudoBean> underlyingPseudoBeanAccessor = new InMemoryAccessor<>(false);

	@Before
	public void before() {
		accessor = new CachedAccessor<AbstractIdentifiableObject>(underlyingAccessor);
		organizableObjectAccessor = new CachedAccessor<AbstractOrganizableObject>(underlyingOrganisableObjectAccessor);
		beanAccessor = new CachedAccessor<Bean>(underlyingBeanAccessor, false);
		underlyingBeanAccessor.enableVersioning(new InMemoryCollection<>(false), 1l);
		pseudoBeanAccessor = new CachedAccessor<>(underlyingPseudoBeanAccessor, false);
		underlyingPseudoBeanAccessor.enableVersioning(new InMemoryCollection<>(false), 1l);
	}
	
	@Test
	public void testCaching() {
		AbstractIdentifiableObject entity = new AbstractOrganizableObject();
		underlyingAccessor.save(entity);
		
		((CachedAccessor<?>)accessor).reloadCache();
		
		AbstractIdentifiableObject actual = accessor.get(entity.getId());
		assertEquals(entity, actual);
		
		AbstractIdentifiableObject entity2 = new AbstractIdentifiableObject();
		accessor.save(entity2);
		
		// Ensure it has been saved to the underlying accessor
		actual = underlyingAccessor.get(entity2.getId());
		assertEquals(entity2, actual);
		
		// Ensure the cache has been updated
		actual = accessor.get(entity2.getId());
		assertEquals(entity2, actual);
		
		accessor.remove(entity2.getId());
		
		// Ensure it has been saved to the underlying accessor
		actual = underlyingAccessor.get(entity2.getId());
		assertNull(actual);
		
		// Ensure the cache has been updated
		actual = accessor.get(entity2.getId());
		assertNull(actual);
	}

}
