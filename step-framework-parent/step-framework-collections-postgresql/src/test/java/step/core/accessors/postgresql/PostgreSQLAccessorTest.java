package step.core.accessors.postgresql;

import org.junit.After;
import org.junit.Before;
import step.core.accessors.AbstractAccessor;
import step.core.accessors.AbstractAccessorTest;
import step.core.accessors.AbstractIdentifiableObject;
import step.core.accessors.AbstractOrganizableObject;
import step.core.collections.Collection;
import step.core.collections.Filters;
import step.core.collections.EntityVersion;
import step.core.collections.postgresql.PostgreSQLCollectionFactory;

import java.io.IOException;

import java.util.Properties;

//Currently no psql server installed on build server and no mocks implemented
public class PostgreSQLAccessorTest extends AbstractAccessorTest {

	PostgreSQLCollectionFactory jdbcDBCollectionFactory;
	@Before
	public void before() throws IOException {
		jdbcDBCollectionFactory = new PostgreSQLCollectionFactory(getProperties());

		accessor = new AbstractAccessor<AbstractIdentifiableObject>(jdbcDBCollectionFactory.getCollection("abstractIdentifiableObject", AbstractIdentifiableObject.class));
		organizableObjectAccessor = new AbstractAccessor<AbstractOrganizableObject>(
				jdbcDBCollectionFactory.getCollection("abstractOrganizableObject", AbstractOrganizableObject.class));
		beanAccessor = new AbstractAccessor<AbstractAccessorTest.Bean>(
				jdbcDBCollectionFactory.getCollection("bean", AbstractAccessorTest.Bean.class));
		Collection<EntityVersion> versionedCollection = jdbcDBCollectionFactory.getVersionedCollection("bean");
		versionedCollection.remove(Filters.empty());
		beanAccessor.enableVersioning(versionedCollection, 1l);
		accessor.getCollectionDriver().remove(Filters.empty());
		organizableObjectAccessor.getCollectionDriver().remove(Filters.empty());
		beanAccessor.getCollectionDriver().remove(Filters.empty());
	}

	@After
	public void after() throws IOException {
		jdbcDBCollectionFactory.close();
	}

	private static Properties getProperties()  {
		Properties properties = new Properties();
		properties.put("jdbcUrl", "jdbc:postgresql://central-postgresql.stepcloud-test.ch/Test");
		properties.put("user", "step");
		properties.put("password", "Jua4Nr!46V");
		return properties;
	}

}
