package step.core.accessors;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class CachedAccessorTest extends AbstractAccessorTest {

	private InMemoryAccessor<AbstractIdentifiableObject> underlyingAccessor = new InMemoryAccessor<>(false);
	private InMemoryAccessor<AbstractOrganizableObject> underlyingOrganisableObjectAccessor = new InMemoryAccessor<>(false);
	private InMemoryAccessor<Bean> underlyingBeanAccessor = new InMemoryAccessor<>(false);
	private InMemoryAccessor<PseudoBean> underlyingPseudoBeanAccessor = new InMemoryAccessor<>(false);

	@Before
	public void before() {
		accessor = new CachedAccessor<AbstractIdentifiableObject>(underlyingAccessor);
		organizableObjectAccessor = new CachedAccessor<AbstractOrganizableObject>(underlyingOrganisableObjectAccessor);
		beanAccessor = new CachedAccessor<Bean>(underlyingBeanAccessor, false);
		pseudoBeanAccessor = new CachedAccessor<>(underlyingPseudoBeanAccessor, false);
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

	@Test
	public void testCachingWithExistingAccessor() {

		InMemoryAccessor<AbstractIdentifiableObject> underlyingBeanAccessor2 = new InMemoryAccessor<>(false);
		AbstractIdentifiableObject entity = new AbstractOrganizableObject();
		underlyingBeanAccessor2.save(entity);

		CachedAccessor<AbstractIdentifiableObject> cachedAccessor2 = new CachedAccessor<>(underlyingBeanAccessor2, false);

		AbstractIdentifiableObject actual = cachedAccessor2.get(entity.getId());
		assertEquals(entity, actual);
	}

}
