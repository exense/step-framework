package step.core.accessors.mongodb;

import org.junit.Before;
import step.core.accessors.AbstractAccessor;
import step.core.accessors.AbstractIdentifiableObject;
import step.core.accessors.AbstractOrganizableObject;
import step.core.accessors.AbstractVersionedAccessorTest;
import step.core.collections.Collection;
import step.core.collections.EntityVersion;
import step.core.collections.Filters;
import step.core.collections.mongodb.MongoDBCollectionFactory;

import java.io.IOException;
import java.util.Properties;

public class MongoDBVersionedAccessorTest extends AbstractVersionedAccessorTest {

	@Before
	public void before() throws IOException {
		MongoDBCollectionFactory mongoDBCollectionFactory = new MongoDBCollectionFactory(getProperties());

		accessor = new AbstractAccessor<AbstractIdentifiableObject>(mongoDBCollectionFactory.getCollection("abstractIdentifiableObject", AbstractIdentifiableObject.class));
		organizableObjectAccessor = new AbstractAccessor<AbstractOrganizableObject>(
				mongoDBCollectionFactory.getCollection("abstractOrganizableObject", AbstractOrganizableObject.class));
		beanAccessor = new AbstractAccessor<Bean>(
				mongoDBCollectionFactory.getCollection("bean", Bean.class));
		accessor.getCollectionDriver().remove(Filters.empty());
		organizableObjectAccessor.getCollectionDriver().remove(Filters.empty());
		beanAccessor.getCollectionDriver().remove(Filters.empty());
		Collection<EntityVersion> versionedBeanCollection = mongoDBCollectionFactory.getVersionedCollection("bean");
		versionedBeanCollection.remove(Filters.empty());
		beanAccessor.enableVersioning(versionedBeanCollection, 1l);
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
}
