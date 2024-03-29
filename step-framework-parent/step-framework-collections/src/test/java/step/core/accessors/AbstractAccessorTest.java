package step.core.accessors;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.Test;

import static org.junit.Assert.*;

public abstract class AbstractAccessorTest {

	protected Accessor<AbstractIdentifiableObject> accessor;
	protected Accessor<AbstractOrganizableObject> organizableObjectAccessor;
	protected Accessor<Bean> beanAccessor;
	protected Accessor<PseudoBean> pseudoBeanAccessor;

	public AbstractAccessorTest() {
		super();
	}

	@Test
	public void testIdentifiableObjectAccessor() {
		AbstractIdentifiableObject entity = new AbstractIdentifiableObject();
		accessor.save(entity);
		AbstractIdentifiableObject actualEntity = accessor.get(entity.getId());
		assertEquals(entity, actualEntity);
		
		actualEntity = accessor.get(entity.getId().toString());
		assertEquals(entity, actualEntity);
		
		List<AbstractIdentifiableObject> range = accessor.getRange(0, 1);
		assertEquals(1, range.size());
		assertEquals(entity, range.get(0));
		
		range = accessor.getRange(10, 1);
		assertEquals(0, range.size());

		List<AbstractIdentifiableObject> all = accessor.stream().collect(Collectors.toList());
		assertEquals(1, all.size());
		
		all.clear();
		accessor.getAll().forEachRemaining(e->all.add(e));
		assertEquals(1, all.size());
		
		all.clear();
		accessor.remove(entity.getId());
		accessor.getAll().forEachRemaining(e->all.add(e));
		assertEquals(0, all.size());
		
		ArrayList<AbstractIdentifiableObject> entities = new ArrayList<AbstractIdentifiableObject>();
		entities.add(new AbstractIdentifiableObject());
		entities.add(new AbstractIdentifiableObject());
		accessor.save(entities);
		accessor.getAll().forEachRemaining(e->all.add(e));
		assertEquals(2, all.size());
		
		entity = new AbstractIdentifiableObject();
		entity.setId(null);
		accessor.save(entity);
		assertNotNull(entity.getId());
	}

    @Test
    public void testFindByIds() {
        AbstractIdentifiableObject entity1 = new AbstractIdentifiableObject();
        AbstractIdentifiableObject entity2 = new AbstractIdentifiableObject();
        AbstractIdentifiableObject entity3 = new AbstractIdentifiableObject();
        AbstractIdentifiableObject entity4 = new AbstractIdentifiableObject();
		accessor.save(entity1);
        accessor.save(entity2);
        accessor.save(entity3);
        // entity 4 is not saved
        List<AbstractIdentifiableObject> foundObjects = accessor.findByIds(Arrays.asList(entity1.getId().toString(), entity2.getId().toString(), entity4.getId().toString())).collect(Collectors.toList());
        assertEquals(2, foundObjects.size());
        assertTrue(foundObjects.stream().anyMatch(o -> o.getId().toString().equals(entity1.getId().toString())));
        assertTrue(foundObjects.stream().anyMatch(o -> o.getId().toString().equals(entity2.getId().toString())));

    }

	@Test
	public void testBeanAccessor() {
		Bean entity = new Bean();
		entity.setProperty1("value 1");
		beanAccessor.save(entity);

		Bean actualEntity = beanAccessor.get(entity.getId());
		assertEquals(entity, actualEntity);

		actualEntity = beanAccessor.get(entity.getId().toString());
		assertEquals(entity, actualEntity);

		actualEntity = beanAccessor.findByCriteria(Map.of("property1", "value 1"));
		assertEquals(entity, actualEntity);

		actualEntity = beanAccessor.findManyByCriteria(Map.of("property1", "value 1")).findFirst().get();
		assertEquals(entity, actualEntity);

		actualEntity = beanAccessor.findByCriteria(new HashMap<> ());
		assertEquals(entity, actualEntity);
	}

	@Test
	public void testPseudoBeanAccessor() {
		PseudoBean entity = new PseudoBean();
		entity.stringField = "stringField1";
		entity.inheritedStringField = "inheritedStringField2";
		entity.anEnum = AnEnum.ONE;
		pseudoBeanAccessor.save(entity);

		PseudoBean actualEntity = pseudoBeanAccessor.get(entity.getId());
		assertEquals(entity, actualEntity);

		actualEntity = pseudoBeanAccessor.get(entity.getId().toString());
		assertEquals(entity, actualEntity);

		actualEntity = pseudoBeanAccessor.findByCriteria(Map.of("stringField", "stringField1"));
		assertEquals(entity, actualEntity);

		actualEntity = pseudoBeanAccessor.findManyByCriteria(Map.of("stringField", "stringField1")).findFirst().get();
		assertEquals(entity, actualEntity);

		actualEntity = pseudoBeanAccessor.findByCriteria(Map.of("inheritedStringField", "inheritedStringField2"));
		assertEquals(entity, actualEntity);

		actualEntity = pseudoBeanAccessor.findManyByCriteria(Map.of("inheritedStringField", "inheritedStringField2")).findFirst().get();
		assertEquals(entity, actualEntity);

		actualEntity = pseudoBeanAccessor.findByCriteria(Map.of("anEnum", AnEnum.ONE.name()));
		assertEquals(entity, actualEntity);

		actualEntity = pseudoBeanAccessor.findManyByCriteria(Map.of("anEnum", AnEnum.ONE.name())).findFirst().get();
		assertEquals(entity, actualEntity);

		actualEntity = pseudoBeanAccessor.findByCriteria(new HashMap<> ());
		assertEquals(entity, actualEntity);
	}

	@Test
	public void testDataIntegrity() {
		Bean bean = new Bean();
		bean.setProperty1("value to save");
		beanAccessor.save(bean);
		bean.setProperty1("local modif");
		Bean beanInDB = beanAccessor.get(bean.getId());
		assertEquals("value to save", beanInDB.getProperty1());
		beanInDB.setProperty1("local modif from retrieved DB bean");
		beanInDB = beanAccessor.get(bean.getId());
		assertEquals("value to save", beanInDB.getProperty1());
	}

	@Test
	public void testFindByAttributes() {
		AbstractOrganizableObject entity = new AbstractOrganizableObject();
		entity.addAttribute("att1", "val1");
		entity.addAttribute("att2", "val2");
		
		testFindByAttributes(organizableObjectAccessor, entity, true);
	}

	@Test
	public void testFindByCustomFields() {
		AbstractOrganizableObject entity = new AbstractOrganizableObject();
		entity.addCustomField("att1", "val1");
		entity.addCustomField("att2", "val2");
		
		testFindByAttributes(organizableObjectAccessor, entity, false);
	}

	private void testFindByAttributes(Accessor<AbstractOrganizableObject> inMemoryAccessor, AbstractOrganizableObject entity, boolean findAttributes) {
		inMemoryAccessor.save(entity);
		
		HashMap<String, String> attributes = new HashMap<>();
		attributes.put("att1", "val1");
		AbstractIdentifiableObject actual = findByAttributes(inMemoryAccessor, findAttributes, attributes);
		assertEquals(entity, actual);
		
		attributes.clear();
		attributes.put("att1", "val2");
		actual = findByAttributes(inMemoryAccessor, findAttributes, attributes);
		assertEquals(null, actual);
		
		actual = findByAttributes(inMemoryAccessor, findAttributes, attributes);
		assertEquals(null, actual);
		
		attributes.clear();
		attributes.put("att1", "val1");
		attributes.put("att2", "val2");
		actual = findByAttributes(inMemoryAccessor, findAttributes, attributes);
		assertEquals(entity, actual);
		
		actual = findByAttributes(inMemoryAccessor, findAttributes, attributes);
		assertEquals(entity, actual);
		
		AbstractOrganizableObject entity2 = new AbstractOrganizableObject();
		inMemoryAccessor.save(entity2);
		
		Spliterator<AbstractOrganizableObject> findManyByAttributes = findManyByAttributes(inMemoryAccessor, findAttributes, null);
		assertEquals(2, StreamSupport.stream(findManyByAttributes, false).collect(Collectors.toList()).size());
		
		findManyByAttributes = findManyByAttributes(inMemoryAccessor, findAttributes, new HashMap<>());
		assertEquals(2, StreamSupport.stream(findManyByAttributes, false).collect(Collectors.toList()).size());
		
		findManyByAttributes = findManyByAttributes(inMemoryAccessor, findAttributes, attributes);
		assertEquals(1, StreamSupport.stream(findManyByAttributes, false).collect(Collectors.toList()).size());
		
		findManyByAttributes = findManyByAttributes(inMemoryAccessor, findAttributes, null);
		assertEquals(2, StreamSupport.stream(findManyByAttributes, false).collect(Collectors.toList()).size());
	}

	private Spliterator<AbstractOrganizableObject> findManyByAttributes(Accessor<AbstractOrganizableObject> inMemoryAccessor, boolean findAttributes, HashMap<String, String> attributes) {
		if(findAttributes) {
			return inMemoryAccessor.findManyByAttributes(attributes, "attributes");
		} else {
			return inMemoryAccessor.findManyByAttributes(attributes, "customFields");
		}
	}

	private AbstractIdentifiableObject findByAttributes(Accessor<AbstractOrganizableObject> inMemoryAccessor, boolean findAttributes, HashMap<String, String> attributes) {
		AbstractIdentifiableObject actual;
		if(findAttributes) {
			actual = inMemoryAccessor.findByAttributes(attributes, "attributes");
		} else {
			actual = inMemoryAccessor.findByAttributes(attributes, "customFields");
		}
		return actual;
	}
	
	public static class Bean extends AbstractIdentifiableObject {
		
		private String property1;

		public String getProperty1() {
			return property1;
		}

		public void setProperty1(String property1) {
			this.property1 = property1;
		}
	}

	public enum AnEnum {
		ZERO,
		ONE,
		TWO
	}
	public static class PseudoBean extends PseudoBeanParentClass {
		public String stringField;
		public AnEnum anEnum;
	}

	public static class PseudoBeanParentClass extends AbstractIdentifiableObject {
		public String inheritedStringField;
	}

}
