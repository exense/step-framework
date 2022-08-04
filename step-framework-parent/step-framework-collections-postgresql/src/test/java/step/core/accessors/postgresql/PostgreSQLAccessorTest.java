package step.core.accessors.postgresql;

import org.junit.Before;
import step.core.accessors.AbstractAccessor;
import step.core.accessors.AbstractAccessorTest;
import step.core.accessors.AbstractIdentifiableObject;
import step.core.accessors.AbstractOrganizableObject;
import step.core.collections.Filters;
import step.core.collections.postgresql.PostgreSQLCollectionFactory;

import java.io.IOException;
import java.util.Properties;

//Currently no psql server installed on build server and no mocks implemented
public class PostgreSQLAccessorTest { //} extends AbstractAccessorTest {

	/*@Before
	public void before() throws IOException {
		PostgreSQLCollectionFactory jdbcDBCollectionFactory = new PostgreSQLCollectionFactory(getProperties());

		accessor = new AbstractAccessor<AbstractIdentifiableObject>(jdbcDBCollectionFactory.getCollection("abstractIdentifiableObject", AbstractIdentifiableObject.class));
		organizableObjectAccessor = new AbstractAccessor<AbstractOrganizableObject>(
				jdbcDBCollectionFactory.getCollection("abstractOrganizableObject", AbstractOrganizableObject.class));
		beanAccessor = new AbstractAccessor<AbstractAccessorTest.Bean>(
				jdbcDBCollectionFactory.getCollection("bean", AbstractAccessorTest.Bean.class));
		accessor.getCollectionDriver().remove(Filters.empty());
		organizableObjectAccessor.getCollectionDriver().remove(Filters.empty());
		beanAccessor.getCollectionDriver().remove(Filters.empty());
	}*/

	private static Properties getProperties()  {
		Properties properties = new Properties();
		properties.put("jdbcUrl", "jdbc:postgresql://localhost/Test");
		properties.put("user", "postgres");
		properties.put("password", "init");
		return properties;
	}

}
