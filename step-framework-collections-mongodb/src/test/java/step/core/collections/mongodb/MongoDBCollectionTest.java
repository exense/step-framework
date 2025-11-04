package step.core.collections.mongodb;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.mongodb.MongoExecutionTimeoutException;
import org.bson.conversions.Bson;
import org.junit.Assert;
import org.junit.Test;

import org.mongojack.JacksonMongoCollection;
import step.core.accessors.AbstractIdentifiableObject;
import step.core.collections.*;
import step.core.entities.Bean;

public class MongoDBCollectionTest extends AbstractCollectionTest {

	public MongoDBCollectionTest() throws IOException {
		super(new MongoDBCollectionFactory(getProperties()));
	}
	
	private static Properties getProperties() throws IOException {
		Properties properties = new Properties();
		properties.put("host", "central-mongodb.stepcloud-test.ch");
		//properties.put("host", "localhost");
		properties.put("database", "test");
		properties.put("username", "tester");
		properties.put("password", "5dB(rs+4YRJe");
		return properties;
	}

	@Test
	public void testDistinct() {
		Collection<Bean> collection = collectionFactory.getCollection("beans", Bean.class);
		collection.remove(Filters.empty());

		Bean bean = new Bean();
		bean.addAttribute("MyAtt1", "My value 1");
		collection.save(bean);
		
		Bean bean2 = new Bean();
		bean2.addAttribute("MyAtt1", "My value 2");
		collection.save(bean2);

		List<Bean> result = collection.find(Filters.empty(), null, null, null, 0).collect(Collectors.toList());
		Bean actualBean = result.get(0);
		assertEquals(bean.getId(), actualBean.getId());
		
		result = collection.find(Filters.empty(), new SearchOrder("MyAtt1", 1), null, null, 0).collect(Collectors.toList());
		assertEquals(bean.getId(), actualBean.getId());

		result = collection.find(Filters.equals(AbstractIdentifiableObject.ID, bean.getId()), null, null, null, 0)
				.collect(Collectors.toList());
		actualBean = result.get(0);
		assertEquals(bean.getId(), actualBean.getId());
		
		
		Collection<Document> documents = collectionFactory.getCollection("beans", Document.class);


		List<Document> documentSearch = documents.find(Filters.empty(), null, null, null, 0).collect(Collectors.toList());
		Document actualDocument = documentSearch.get(0);
		assertEquals(bean.getId().toString(), actualDocument.get(AbstractIdentifiableObject.ID));
		
		List<String> ids = documents.distinct(AbstractIdentifiableObject.ID, Filters.empty()).stream().collect(Collectors.toList());
		assertEquals(bean.getId().toString(), ids.get(0));
		
		List<String> distinct = documents.distinct("attributes.MyAtt1", Filters.empty()).stream().collect(Collectors.toList());
		assertEquals("My value 1", distinct.get(0));
		
		documents.remove(Filters.empty());
		assertEquals(0, documents.find(Filters.empty(), null, null, null, 0).collect(Collectors.toList()).size());
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testTimeout() throws Exception {
		// This simulates the same condition as the production code might run into when queries take too long,
		// but here we're using an explicit query that will force a timeout. We can't really run the production
		// code with a custom query without major hackery, so we just simulate something very close to it.
		// I know it's ugly, but it's the best I could come up with.
		Collection<Document> collection = collectionFactory.getCollection("beans", Document.class);
		Field f = collection.getClass().getDeclaredField("collection");
		f.setAccessible(true);
		JacksonMongoCollection<org.bson.Document> mc = (JacksonMongoCollection) f.get(collection);
		Bson query = new org.bson.Document("$where", "sleep(3000) || true");
		try {
			mc.find(query).maxTime(1, TimeUnit.SECONDS).into(new ArrayList<>());
			Assert.fail("This should never be reached");
		} catch (MongoExecutionTimeoutException e) {
			// This is the same format that detailed log messages use (in prod):
			String msg = String.format("MongoDB query timed out after %d seconds: %s", 1, query);
			assertEquals("MongoDB query timed out after 1 seconds: Document{{$where=sleep(3000) || true}}", msg);
		}
	}

}
