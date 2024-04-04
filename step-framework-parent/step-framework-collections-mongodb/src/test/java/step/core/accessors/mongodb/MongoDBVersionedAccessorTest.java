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

import java.util.Properties;

public class MongoDBVersionedAccessorTest extends AbstractVersionedAccessorTest {

	@Before
	public void before() {
		MongoDBCollectionFactory mongoDBCollectionFactory = new MongoDBCollectionFactory(getProperties());

		accessor = new AbstractAccessor<>(mongoDBCollectionFactory.getCollection("abstractIdentifiableObject", AbstractIdentifiableObject.class));
		organizableObjectAccessor = new AbstractAccessor<>(mongoDBCollectionFactory.getCollection("abstractOrganizableObject", AbstractOrganizableObject.class));
		beanAccessor = new AbstractAccessor<>(mongoDBCollectionFactory.getCollection("bean", Bean.class));
		pseudoBeanAccessor = new AbstractAccessor<>(mongoDBCollectionFactory.getCollection("pseudoBean", PseudoBean.class));
		accessor.getCollectionDriver().remove(Filters.empty());
		organizableObjectAccessor.getCollectionDriver().remove(Filters.empty());
		beanAccessor.getCollectionDriver().remove(Filters.empty());
		pseudoBeanAccessor.getCollectionDriver().remove(Filters.empty());
		Collection<EntityVersion> versionedBeanCollection = mongoDBCollectionFactory.getVersionedCollection("bean");
		versionedBeanCollection.remove(Filters.empty());
		Collection<EntityVersion> versionedPseudoBeanCollection = mongoDBCollectionFactory.getVersionedCollection("pseudoBean");
		versionedPseudoBeanCollection.remove(Filters.empty());
		beanAccessor.enableVersioning(versionedBeanCollection, 1L);
		pseudoBeanAccessor.enableVersioning(versionedPseudoBeanCollection, 1L);
	}


	private static Properties getProperties() {
		Properties properties = new Properties();
		properties.put("host", "central-mongodb.stepcloud-test.ch");
		//properties.put("host", "localhost");
		properties.put("database", "test");
		properties.put("username", "tester");
		properties.put("password", "5dB(rs+4YRJe");
		return properties;
	}
}
