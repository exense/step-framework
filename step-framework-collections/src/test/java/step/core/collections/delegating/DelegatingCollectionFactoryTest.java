package step.core.collections.delegating;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import step.core.collections.Collection;
import step.core.collections.CollectionFactory;
import step.core.collections.EntityVersion;
import step.core.collections.inmemory.InMemoryCollection;

public class DelegatingCollectionFactoryTest {

	@Test
	public void test() throws IOException {
		InMemoryCollection<Object> collectionFromFactory1 = new InMemoryCollection<Object>();
		InMemoryCollection<EntityVersion> collectionFromFactory1Versioned = new InMemoryCollection<>();
		CollectionFactory collectionFactory1 = newCollectionFactory(collectionFromFactory1, collectionFromFactory1Versioned);

		InMemoryCollection<Object> collectionFromFactory2 = new InMemoryCollection<Object>();
		CollectionFactory collectionFactory2 = newCollectionFactory(collectionFromFactory2, null);

		DelegatingCollectionFactory collectionFactory = new DelegatingCollectionFactory();
		collectionFactory.addCollectionFactory("factory1", collectionFactory1);
		collectionFactory.addCollectionFactory("factory2", collectionFactory2);

		collectionFactory.addRoute("myCollection1", "factory1");
		collectionFactory.addRoute("myCollection2", "factory2");
		collectionFactory.setDefaultRoute("factory2");

		Collection<Object> actualCollection = collectionFactory.getCollection("myCollection1", null);
		Assert.assertTrue(actualCollection == collectionFromFactory1);

		Collection<EntityVersion> actualVersionedCollection = collectionFactory.getVersionedCollection("myCollection1");
		Assert.assertTrue(actualVersionedCollection == collectionFromFactory1Versioned);

		actualCollection = collectionFactory.getCollection("myCollection2", null);
		Assert.assertTrue(actualCollection == collectionFromFactory2);

		actualCollection = collectionFactory.getCollection("myCollection3", null);
		Assert.assertTrue(actualCollection == collectionFromFactory2);

		// Resetting default route and retrieving a collection for which no route exist
		collectionFactory.setDefaultRoute(null);
		Exception actualException = null;
		try {
			collectionFactory.getCollection("myOtherCollection", null);
		} catch (Exception e) {
			actualException = e;
		}
		Assert.assertNotNull(actualException);

		// Defining an invalid route
		actualException = null;
		try {
			collectionFactory.addRoute("myCollection1", "invalidCollectionId");
		} catch (Exception e) {
			actualException = e;
		}
		Assert.assertNotNull(actualException);

		collectionFactory.close();
	}

	private CollectionFactory newCollectionFactory(InMemoryCollection<Object> collectionFromFactory1,
												   InMemoryCollection<EntityVersion>  collectionFromFactory1Versioned) {
		return new CollectionFactory() {

			@Override
			public void close() throws IOException {
			}

			@SuppressWarnings("unchecked")
			@Override
			public <T> Collection<T> getCollection(String name, Class<T> entityClass) {
				return (Collection<T>) collectionFromFactory1;
			}

			@Override
			public Collection<EntityVersion> getVersionedCollection(String name) {
				return (Collection<EntityVersion>) collectionFromFactory1Versioned;
			}
		};
	}

}
