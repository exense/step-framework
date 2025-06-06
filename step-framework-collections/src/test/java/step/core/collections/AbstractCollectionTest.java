package step.core.collections;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.json.Json;
import jakarta.json.JsonObject;

import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;

import step.core.accessors.AbstractIdentifiableObject;
import step.core.accessors.DefaultJacksonMapperProvider;
import step.core.collections.serialization.DottedKeyMap;
import step.core.entities.Bean;
import step.core.entities.SimpleBean;

import static org.junit.Assert.*;

public abstract class AbstractCollectionTest {

	private static final String COLLECTION = "beans";
	private static final String NEW_VALUE = "newValue";
	private static final String VALUE1 = "Test1";
	private static final String VALUE2 = "Test2";
	private static final String VALUE3 = "Test3";
	private static final String VALUE_SERIALIZEDLIST1 = ";one=un;two=deux;three=trois;four=trois;";
	private static final String VALUE_SERIALIZEDLIST2 = ";two=deux;three=drei;four=deux;";
	private static final String VALUE_SERIALIZEDLIST3 = ";;";
	private static final String PROPERTY1 = "property1";

	protected final CollectionFactory collectionFactory;

	public AbstractCollectionTest(CollectionFactory collectionFactory) {
		super();
		this.collectionFactory = collectionFactory;
	}
	@After
	public void tearDown() throws IOException {
		collectionFactory.close();
	}

	@Test
	public void testCounts() throws Exception {
		// Bean collection
		Collection<Bean> beanCollection = collectionFactory.getCollection(COLLECTION, Bean.class);
		beanCollection.remove(Filters.empty());
		
		long count = beanCollection.count(Filters.empty(), 10);
		assertEquals(0, count);
		
		count = beanCollection.count(Filters.regex("dummy", "dummy", false), 10);
		assertEquals(0, count);
		
		count = beanCollection.estimatedCount();
		assertTrue(-1 <= count && count <= 1); //psql estimate are based on stats can be -1
		
		Bean bean1 = new Bean(VALUE1);
		beanCollection.save(bean1);

		count = beanCollection.estimatedCount();
		assertTrue(-1 <= count && count <= 1); //psql estimate are based on stats and can be -1
		
		count = beanCollection.count(Filters.empty(), 10);
		assertEquals(1, count);
		
		Bean bean2 = new Bean(VALUE1);
		beanCollection.save(bean2);
		
		count = beanCollection.count(Filters.empty(), 10);
		assertEquals(2, count);
		
		count = beanCollection.count(Filters.empty(), 1);
		assertEquals(1, count);
	}
	
	@Test
	public void testGetById() throws Exception {
		// Bean collection
		Collection<Bean> beanCollection = collectionFactory.getCollection(COLLECTION, Bean.class);
		beanCollection.remove(Filters.empty());
		Bean bean1 = new Bean(VALUE1);
		SimpleBean simpleBean = new SimpleBean();
		simpleBean.setId("testId");
		bean1.setSimpleBean(simpleBean);
		beanCollection.save(bean1);

		// Id as ObjectId
		Bean actualBean = beanCollection.find(Filters.id(bean1.getId()), null, null, null, 0).findFirst().get();
		assertEquals(bean1, actualBean);

		// Id as string
		actualBean = beanCollection.find(Filters.id(bean1.getId().toString()), null, null, null, 0).findFirst().get();
		assertEquals(bean1, actualBean);

		// Id as ObjectId in equals filter
		actualBean = beanCollection
				.find(Filters.equals(AbstractIdentifiableObject.ID, bean1.getId()), null, null, null, 0).findFirst()
				.get();
		assertEquals(bean1, actualBean);

		// Id as string in equals filter
		actualBean = beanCollection
				.find(Filters.equals(AbstractIdentifiableObject.ID, bean1.getId().toString()), null, null, null, 0)
				.findFirst().get();
		assertEquals(bean1, actualBean);
		
		// Id as string for nested bean in equals filter
		// In that case the id shouldn't be converted implicitly to ObjectId
		actualBean = beanCollection
				.find(Filters.equals("simpleBean.id", "testId"), null, null, null, 0)
				.findFirst().get();
		assertEquals(bean1, actualBean);

		// Document collection
		Collection<Document> documentCollection = collectionFactory.getCollection(COLLECTION, Document.class);

		// Id as ObjectId
		Document actualDocument = documentCollection.find(Filters.id(bean1.getId()), null, null, null, 0).findFirst()
				.get();
		assertEquals(bean1.getId(), actualDocument.getId());

		// Id as ObjectId
		actualDocument = documentCollection.find(Filters.id(bean1.getId().toString()), null, null, null, 0).findFirst()
				.get();
		assertEquals(bean1.getId(), actualDocument.getId());
	}

	@Test
	public void test() throws Exception {
		Collection<Bean> beanCollection = collectionFactory.getCollection(COLLECTION, Bean.class);
		assertEquals(COLLECTION, beanCollection.getName());
		beanCollection.remove(Filters.empty());

		Bean bean1 = new Bean(VALUE1);
		beanCollection.save(bean1);
		Collection<Document> mapCollection = collectionFactory.getCollection(COLLECTION, Document.class);

		Document document = mapCollection.find(Filters.equals(PROPERTY1, VALUE1), null, null, null, 0).findFirst()
				.get();

		Object id = document.get(AbstractIdentifiableObject.ID);
		assertTrue(id instanceof String);
		assertEquals(bean1.getId(), document.getId());
		assertEquals(VALUE1, document.get(PROPERTY1));

		// The returned document should be convertible to the bean with the default mapper
		Bean bean = DefaultJacksonMapperProvider.getObjectMapper().convertValue(document, Bean.class);
		assertEquals(bean1, bean);

		document.put(PROPERTY1, NEW_VALUE);
		mapCollection.save(document);

		Bean actualBean = beanCollection
				.find(Filters.equals(AbstractIdentifiableObject.ID,
						new ObjectId(document.get(AbstractIdentifiableObject.ID).toString())), null, null, null, 0)
				.findFirst().get();
		assertEquals(NEW_VALUE, actualBean.getProperty1());
	}

	@Test
	public void testSave() throws Exception {
		// Bean collection
		Collection<Bean> beanCollection = collectionFactory.getCollection(COLLECTION, Bean.class);
		beanCollection.remove(Filters.empty());

		Bean bean1 = new Bean(VALUE1);
		// Reset id
		bean1.setId(null);
		beanCollection.save(bean1);
		Bean bean1AfterSave = beanCollection.save(bean1);
		// An Id should have been generated
		assertNotNull(bean1AfterSave.getId());

		// Document collection
		Collection<Document> documentCollection = collectionFactory.getCollection(COLLECTION, Document.class);
		documentCollection.remove(Filters.empty());
		Document document = new Document();
		document.put("_class", Bean.class.getName());
		document = documentCollection.save(document);
		assertNotNull(document.get(AbstractIdentifiableObject.ID));
	}

	@Test
	public void testFind() throws Exception {
		Collection<Bean> beanCollection = collectionFactory.getCollection(COLLECTION, Bean.class);
		beanCollection.remove(Filters.empty());

		Bean bean1 = new Bean(VALUE1);
		Bean bean2 = new Bean(VALUE2);
		Bean bean3 = new Bean(VALUE3);
		beanCollection.save(List.of(bean1, bean3, bean2));

		// Sort ascending
		List<Bean> result = beanCollection.find(Filters.empty(), new SearchOrder(PROPERTY1, 1), null, null, 0)
				.collect(Collectors.toList());
		assertEquals(List.of(bean1, bean2, bean3), result);

		// Sort ascending by ID
		result = beanCollection.find(Filters.empty(), new SearchOrder(AbstractIdentifiableObject.ID, 1), null, null, 0)
				.collect(Collectors.toList());
		assertEquals(List.of(bean1, bean2, bean3), result);

		// Sort descending
		result = beanCollection.find(Filters.empty(), new SearchOrder(PROPERTY1, -1), null, null, 0)
				.collect(Collectors.toList());
		assertEquals(List.of(bean3, bean2, bean1), result);

		// Skip limit
		result = beanCollection.find(Filters.empty(), new SearchOrder(PROPERTY1, 1), 1, 2, 0)
				.collect(Collectors.toList());
		assertEquals(List.of(bean2, bean3), result);
	}

	@Test
	public void testFindCloseable() throws Exception {
		Collection<Bean> beanCollection = collectionFactory.getCollection(COLLECTION, Bean.class);
		beanCollection.remove(Filters.empty());

		Bean bean1 = new Bean(VALUE1);
		Bean bean2 = new Bean(VALUE2);
		Bean bean3 = new Bean(VALUE3);
		beanCollection.save(List.of(bean1, bean3, bean2));

		// Sort ascending
		List<Bean> result;
		try (Stream<Bean> beanStream = beanCollection.findLazy(Filters.empty(), new SearchOrder(PROPERTY1, 1), null, null, 0)) {
			result = beanStream.collect(Collectors.toList());
		}
		assertEquals(List.of(bean1, bean2, bean3), result);

		// Sort ascending by ID
		try (Stream<Bean> beanStream = beanCollection.findLazy(Filters.empty(), new SearchOrder(AbstractIdentifiableObject.ID, 1), null, null, 0)) {
			result = beanStream.collect(Collectors.toList());
		}
		assertEquals(List.of(bean1, bean2, bean3), result);

		// Sort descending
		try (Stream<Bean> beanStream = beanCollection.findLazy(Filters.empty(), new SearchOrder(PROPERTY1, -1), null, null, 0)) {
			result = beanStream.collect(Collectors.toList());
		}
		assertEquals(List.of(bean3, bean2, bean1), result);

		// Skip limit
		try (Stream<Bean> beanStream = beanCollection.findLazy(Filters.empty(), new SearchOrder(PROPERTY1, 1), 1, 2, 0)) {
			result = beanStream.collect(Collectors.toList());
		}
		assertEquals(List.of(bean2, bean3), result);
	}
	
	@Test
	public void testFindFilters() {
		Collection<Bean> collection = collectionFactory.getCollection("beans", Bean.class);
		collection.remove(Filters.empty());
		
		Bean bean = new Bean();
		bean.setProperty1("My property 1");
		bean.setLongProperty(11l);
		bean.setBooleanProperty(false);
		bean.addAttribute("MyAtt1", "My value 1");
		collection.save(bean);

		Bean bean2 = new Bean();
		bean2.setProperty1("My property 2");
		bean2.setLongProperty(21l);
		bean2.setBooleanProperty(false);
		bean2.addAttribute("MyAtt1", "My value 2");
		bean2.addAttribute("MyAtt2", "My other value");
		collection.save(bean2);
		
		// Find by regex
		List<Bean> result = collection.find(Filters.regex("property1", "My", true), new SearchOrder("MyAtt1", 1), null, null, 0).collect(Collectors.toList());
		assertEquals(bean.getId(), result.get(0).getId());
		
		// Find by regex with dotted key
		result = collection.find(Filters.regex("attributes.MyAtt1", "My", true), new SearchOrder("MyAtt1", 1), null, null, 0).collect(Collectors.toList());
		assertEquals(bean.getId(), result.get(0).getId());
		
		// Equals String
		result = collection.find(Filters.equals("property1", "My property 1"), new SearchOrder("MyAtt1", 1), null, null, 0).collect(Collectors.toList());
		assertEquals(bean.getId(), result.get(0).getId());
		
		// Equals String with dotted key
		result = collection.find(Filters.equals("attributes.MyAtt1", "My value 1"), new SearchOrder("MyAtt1", 1), null, null, 0).collect(Collectors.toList());
		assertEquals(bean.getId(), result.get(0).getId());
		
		// Equals boolean
		result = collection.find(Filters.equals("booleanProperty", false), new SearchOrder("MyAtt1", 1), null, null, 0).collect(Collectors.toList());
		assertEquals(bean.getId(), result.get(0).getId());
		
		// Equals long
		result = collection.find(Filters.equals("longProperty", 11), new SearchOrder("MyAtt1", 1), null, null, 0).collect(Collectors.toList());
		assertEquals(bean.getId(), result.get(0).getId());

		//is null
		result = collection.find(Filters.equals("missingField", (String) null), new SearchOrder("MyAtt1", 1), null, null, 0).collect(Collectors.toList());
		assertEquals(bean.getId(), result.get(0).getId());

		result = collection.find(Filters.and(List.of(Filters.gte("longProperty", 11),Filters.lt("longProperty",21))), new SearchOrder("MyAtt1", 1), null, null, 0).collect(Collectors.toList());
		assertEquals(bean.getId(), result.get(0).getId());
		assertEquals(1, result.size());

		result = collection.find(Filters.and(List.of(Filters.gt("longProperty", 11),Filters.lte("longProperty",21))), new SearchOrder("MyAtt1", 1), null, null, 0).collect(Collectors.toList());
		assertEquals(bean2.getId(), result.get(0).getId());
		assertEquals(1, result.size());

		//Not equal
		result = collection.find(Filters.not(Filters.equals("attributes.MyAtt2", "My value 1")), new SearchOrder("MyAtt1", 1), null, null, 0).collect(Collectors.toList());
		assertEquals(2, result.size());

		//Exists
		result = collection.find(Filters.exists("attributes.MyAtt2"), null, null, null, 0).collect(Collectors.toList());
		assertEquals(1, result.size());
		assertEquals("My value 2", result.get(0).getAttribute("MyAtt1"));


		//Does not exists
		result = collection.find(Filters.not(Filters.exists("attributes.MyAtt2")), null, null, null, 0).collect(Collectors.toList());
		assertEquals(1, result.size());
		assertEquals("My value 1", result.get(0).getAttribute("MyAtt1"));

		//Test errors for empty AND and OR filters
		assertThrows(Throwable.class, () -> collection.find(Filters.and(new ArrayList<>()), null, null, null, 0).collect(Collectors.toList()));

		assertThrows(Throwable.class, () -> collection.find(Filters.or(new ArrayList<>()), null, null, null, 0).collect(Collectors.toList()));
	}

	@Test
	public void testFindSearchOrdersFilters() {
		Collection<Bean> collection = collectionFactory.getCollection("beans", Bean.class);
		collection.remove(Filters.empty());

		Bean bean4 = new Bean();
		bean4.setProperty1("bean4");
		bean4.addAttribute("MyAtt1", "CCC");
		bean4.addAttribute("MyAtt2", "AAA");
		collection.save(bean4);

		Bean bean2 = new Bean();
		bean2.setProperty1("bean2");
		bean2.addAttribute("MyAtt1", "AAA");
		bean2.addAttribute("MyAtt2", "CCC");
		collection.save(bean2);

		Bean bean3 = new Bean();
		bean3.setProperty1("bean3");
		bean3.addAttribute("MyAtt1", "BBB");
		bean3.addAttribute("MyAtt2", "DDD");
		collection.save(bean3);

		Bean bean1 = new Bean();
		bean1.setProperty1("bean1");
		bean1.addAttribute("MyAtt1", "AAA");
		bean1.addAttribute("MyAtt2", "BBB");
		collection.save(bean1);

		// No sort
		List<Bean> result = collection.find(Filters.empty(),null, null, null, 0).collect(Collectors.toList());
		assertEquals("bean4", result.get(0).getProperty1());
		assertEquals("bean2", result.get(1).getProperty1());
		assertEquals("bean3", result.get(2).getProperty1());
		assertEquals("bean1", result.get(3).getProperty1());

		// Simple sort on one attributes.MyAtt1
		result = collection.find(Filters.empty(),new SearchOrder("attributes.MyAtt1", 1), null, null, 0).collect(Collectors.toList());
		assertEquals("bean2", result.get(0).getProperty1());
		assertEquals("bean1", result.get(1).getProperty1());
		assertEquals("bean3", result.get(2).getProperty1());
		assertEquals("bean4", result.get(3).getProperty1());

		// Simple sort on one attributes.MyAtt2
		result = collection.find(Filters.empty(),new SearchOrder("attributes.MyAtt2", 1), null, null, 0).collect(Collectors.toList());
		assertEquals("bean4", result.get(0).getProperty1());
		assertEquals("bean1", result.get(1).getProperty1());
		assertEquals("bean2", result.get(2).getProperty1());
		assertEquals("bean3", result.get(3).getProperty1());

		// Combined sort on one attributes.MyAtt1 and attributes.MyAtt2
		SearchOrder searchOrder = new SearchOrder(List.of(new SearchOrder.FieldSearchOrder("attributes.MyAtt1", 1),
				new SearchOrder.FieldSearchOrder("attributes.MyAtt2", 1)));
		result = collection.find(Filters.empty(), searchOrder, null, null, 0).collect(Collectors.toList());
		assertEquals("bean1", result.get(0).getProperty1());
		assertEquals("bean2", result.get(1).getProperty1());
		assertEquals("bean3", result.get(2).getProperty1());
		assertEquals("bean4", result.get(3).getProperty1());

		// Combined sort on one attributes.MyAtt2 and attributes.MyAtt1
		searchOrder = new SearchOrder(List.of(new SearchOrder.FieldSearchOrder("attributes.MyAtt2", 1),
				new SearchOrder.FieldSearchOrder("attributes.MyAtt1", 1)));
		result = collection.find(Filters.empty(), searchOrder, null, null, 0).collect(Collectors.toList());
		assertEquals("bean4", result.get(0).getProperty1());
		assertEquals("bean1", result.get(1).getProperty1());
		assertEquals("bean2", result.get(2).getProperty1());
		assertEquals("bean3", result.get(3).getProperty1());

		// Combined sort on one attributes.MyAtt1 and attributes.MyAtt2 desc
		searchOrder = new SearchOrder(List.of(new SearchOrder.FieldSearchOrder("attributes.MyAtt1", 1),
				new SearchOrder.FieldSearchOrder("attributes.MyAtt2", -1)));
		result = collection.find(Filters.empty(), searchOrder, null, null, 0).collect(Collectors.toList());
		assertEquals("bean2", result.get(0).getProperty1());
		assertEquals("bean1", result.get(1).getProperty1());
		assertEquals("bean3", result.get(2).getProperty1());
		assertEquals("bean4", result.get(3).getProperty1());
	}

	@Test
	public void testFindComplexRegexFilters() {
		Collection<Bean> collection = collectionFactory.getCollection("beans", Bean.class);
		collection.remove(Filters.empty());

		Bean bean1 = new Bean(VALUE_SERIALIZEDLIST1);

		Bean bean2 = new Bean(VALUE_SERIALIZEDLIST2);

		Bean bean3 = new Bean(VALUE_SERIALIZEDLIST3);

		collection.save(List.of(bean1, bean2, bean3));

		// Simple substring, actually
		List<Bean> result = collection.find(Filters.regex("property1", ";two=deux;", true), null, null, null, 0).collect(Collectors.toList());
		assertEquals(2, result.size());

		result = collection.find(Filters.regex("property1", ";two=(.+?);", true), null, null, null, 0).collect(Collectors.toList());
		assertEquals(2, result.size());
		result = collection.find(Filters.regex("property1", ";two=[^;]+;", true), null, null, null, 0).collect(Collectors.toList());
		assertEquals(2, result.size());
		result = collection.find(Filters.regex("property1", ";three=[^;]+;", true), null, null, null, 0).collect(Collectors.toList());
		assertEquals(2, result.size());
		result = collection.find(Filters.regex("property1", ";(.*?)=(.*?);", true), null, null, null, 0).collect(Collectors.toList());
		assertEquals(2, result.size());

		// more complicated way using positive lookahead (yes, the syntax is mind-bending).
		result = collection.find(Filters.regex("property1", ";three=(?=(trois;))", true), null, null, null, 0).collect(Collectors.toList());
		assertEquals(1, result.size());
		assertEquals(bean1.getId(), result.get(0).getId());
		// and... negative lookahead: entries containing "three=...", but not with value "trois/deux".
		result = collection.find(Filters.regex("property1", ";three=(?!(trois;))", true), null, null, null, 0).collect(Collectors.toList());
		assertEquals(1, result.size());
		assertEquals(bean2.getId(), result.get(0).getId());
		result = collection.find(Filters.regex("property1", ";three=(?!(deux;))", true), null, null, null, 0).collect(Collectors.toList());
		assertEquals(2, result.size());
		// negative lookahead, no "three=..." at all
		result = collection.find(Filters.regex("property1", "^((?!;three=).)*$", true), null, null, null, 0).collect(Collectors.toList());
		assertEquals(1, result.size());
		assertEquals(bean3.getId(), result.get(0).getId());

	}

	@Test
	public void testFindBySpecialFilters() throws Exception {
		// Bean collection
		Collection<Bean> beanCollection = collectionFactory.getCollection(COLLECTION, Bean.class);
		beanCollection.remove(Filters.empty());
		Bean bean1 = new Bean(VALUE1);
		bean1.setNested(new Bean());
		beanCollection.save(bean1);

		// Special field _class
		Bean actualBean = beanCollection.find(Filters.regex("_class", "Bean", true), null, null, null, 0).findFirst().get();
		assertEquals(bean1, actualBean);

		// Special field _class nested
		beanCollection.find(Filters.regex("nested._class", "Bean", true), null, null, null, 0).findFirst().get();
		assertEquals(bean1, actualBean);

		// Special field id
		actualBean = beanCollection.find(Filters.equals("id", bean1.getId()), null, null, null, 0).findFirst().get();
		assertEquals(bean1, actualBean);
	}

	@Test
	public void testFindSpecialChars() {
		Collection<Bean> collection = collectionFactory.getCollection("beans", Bean.class);
		collection.remove(Filters.empty());

		Bean bean1 = new Bean("test'test");
		collection.save(List.of(bean1));

		List<Bean> result = collection.find(Filters.regex("property1", "test'test", true), null, null, null, 0).collect(Collectors.toList());
		assertEquals(1, result.size());
	}

	@Test
	public void testRemove() throws Exception {
		Collection<Bean> beanCollection = collectionFactory.getCollection(COLLECTION, Bean.class);
		beanCollection.remove(Filters.empty());

		beanCollection.save(new Bean(VALUE1));

		beanCollection.remove(Filters.equals(PROPERTY1, VALUE1));

		assertNull(beanCollection.find(Filters.equals(PROPERTY1, VALUE1), new SearchOrder("property", 1), null, null, 0)
				.findFirst().orElse(null));
	}

	@Test
	public void testSerializers() throws Exception {
		Collection<Bean> beanCollection = collectionFactory.getCollection(COLLECTION, Bean.class);
		beanCollection.remove(Filters.empty());

		Bean bean1 = new Bean(VALUE1);
		// JSR353
		JsonObject json = Json.createObjectBuilder().add("test", "value").build();
		bean1.setJsonObject(json);
		// org.json
		JSONObject jsonOrgObject = new JSONObject();
		jsonOrgObject.put("key", "value");
		bean1.setJsonOrgObject(jsonOrgObject);
		// map with dots in keys
		DottedKeyMap<String, String> map = new DottedKeyMap<String, String>();
		map.put("key.with.dots", "value");
		bean1.setMap(map);

		beanCollection.save(bean1);

		Bean actualBean = beanCollection.find(Filters.id(bean1.getId()), null, null, null, 0).findFirst().get();
		assertEquals(json, actualBean.getJsonObject());
		// JSONObject doesn't implement equals()
		assertEquals("value", actualBean.getJsonOrgObject().get("key"));
		assertEquals("value", actualBean.getMap().get("key.with.dots"));
	}

	@Test
	public void testDrop() {
		Collection<Bean> beanCollection = collectionFactory.getCollection(COLLECTION, Bean.class);
		beanCollection.remove(Filters.empty());
		beanCollection.save(new Bean(VALUE1));
		assertEquals(1, beanCollection.count(Filters.empty(), null));
		beanCollection.drop();
		beanCollection = collectionFactory.getCollection(COLLECTION, Bean.class);
		assertEquals(0, beanCollection.count(Filters.empty(), null));
	}

	@Test
	public void renameCollections() {
		collectionFactory.getCollection("beans", Bean.class).drop();
		collectionFactory.getCollection("beansrenamed", Bean.class).drop();

		Collection<Bean> collection = collectionFactory.getCollection("beans", Bean.class);
		assertEquals("beans", collection.getName());
		Bean bean = new Bean();
		bean.addAttribute("MyAtt1", "My value 1");
		collection.save(bean);

		List<Bean> result = collection.find(Filters.empty(), null, null, null, 0).collect(Collectors.toList());
		Bean actualBean = result.get(0);
		assertEquals(bean.getId(), actualBean.getId());

		collection.rename("beansrenamed");
		// Assert that the collection has been renamed properly
		assertEquals("beansrenamed", collection.getName());

		// Assert that the bean is still present in the previous instance of the collection
		result = collection.find(Filters.empty(), null, null, null, 0).collect(Collectors.toList());
		actualBean = result.get(0);
		assertEquals(bean.getId(), actualBean.getId());

		// Assert that the bean is found when recreating a collection with the new name
		Collection<Bean> collectionRenamed = collectionFactory.getCollection("beansrenamed", Bean.class);
		result = collectionRenamed.find(Filters.empty(), null, null, null, 0).collect(Collectors.toList());
		actualBean = result.get(0);
		assertEquals(bean.getId(), actualBean.getId());

		collection = collectionFactory.getCollection("beans", Bean.class);
		result = collection.find(Filters.empty(), null, null, null, 0).collect(Collectors.toList());
		assertEquals(0, result.size());
	}
}
