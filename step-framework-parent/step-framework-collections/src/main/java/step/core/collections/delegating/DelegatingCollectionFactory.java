package step.core.collections.delegating;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import step.core.collections.Collection;
import step.core.collections.CollectionFactory;
import step.core.collections.EntityVersion;

public class DelegatingCollectionFactory implements CollectionFactory {

	private final Logger logger = LoggerFactory.getLogger(DelegatingCollectionFactory.class);

	private final Map<String, CollectionFactory> collectionFactories = new ConcurrentHashMap<>();

	private final Map<String, String> routes = new ConcurrentHashMap<>();
	private String defaultCollectionFactory;

	public void addCollectionFactory(String collectionId, CollectionFactory collectionFactory) {
		collectionFactories.put(collectionId, collectionFactory);
	}

	public void addRoute(String collectionName, String collectionId) {
		assertCollectionIdExists(collectionId);
		routes.put(collectionName, collectionId);
	}

	public void setDefaultRoute(String collectionId) {
		if (collectionId != null) {
			assertCollectionIdExists(collectionId);
		}
		defaultCollectionFactory = collectionId;
	}

	private void assertCollectionIdExists(String collectionId) {
		if (!collectionFactories.containsKey(collectionId)) {
			throw new IllegalArgumentException("The collection " + collectionId + " doesn't exist");
		}
	}

	@Override
	public <T> Collection<T> getCollection(String name, Class<T> entityClass) {
		String collectionId = routes.getOrDefault(name, defaultCollectionFactory);
		if (collectionId == null) {
			throw new RuntimeException("No route found for collection " + name);
		}
		CollectionFactory collectionFactory = collectionFactories.get(collectionId);
		return collectionFactory.getCollection(name, entityClass);
	}

	@Override
	public Collection<EntityVersion> getVersionedCollection(String name) {
		String collectionId = routes.getOrDefault(name, defaultCollectionFactory);
		if (collectionId == null) {
			throw new RuntimeException("No route found for collection " + name);
		}
		CollectionFactory collectionFactory = collectionFactories.get(collectionId);
		return collectionFactory.getVersionedCollection(name);
	}

	@Override
	public void close() throws IOException {
		collectionFactories.values().forEach(t -> {
			try {
				t.close();
			} catch (IOException e) {
				logger.error("Error while closing collection factory", e);
			}
		});
	}
}
