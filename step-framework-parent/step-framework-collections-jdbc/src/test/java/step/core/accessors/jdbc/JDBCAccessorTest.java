package step.core.accessors.jdbc;

import org.junit.Before;
import step.core.accessors.AbstractAccessor;
import step.core.accessors.AbstractAccessorTest;
import step.core.accessors.AbstractIdentifiableObject;
import step.core.accessors.AbstractOrganizableObject;
import step.core.collections.Filters;
import step.core.collections.jdbc.JdbcCollectionFactory;

import java.io.IOException;
import java.util.Properties;

public class JDBCAccessorTest {//extends AbstractAccessorTest {

	/*@Before
	public void before() throws IOException {
		JdbcCollectionFactory jdbcDBCollectionFactory = new JdbcCollectionFactory(getProperties());

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
		properties.put("url", "jdbc:postgresql://localhost/Test");
		properties.put("user", "postgres");
		properties.put("password", "init");
		return properties;
	}

}
