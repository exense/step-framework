package step.framework.server;

import step.core.AbstractContext;
import step.core.accessors.AbstractIdentifiableObject;
import step.core.accessors.Accessor;

public class ServerContext extends AbstractContext {

	@SuppressWarnings("unchecked")
	public <T extends AbstractIdentifiableObject> Accessor<T> getAccessor(Class<T> beanClass) {
		return (Accessor<T>) get(accessorKey(beanClass));
	}
	
	public <T extends AbstractIdentifiableObject> void putAccessor(Class<T> beanClass, Accessor<T> accessor) {
		put(accessorKey(beanClass), accessor);
	}

	private <T extends AbstractIdentifiableObject> String accessorKey(Class<T> beanClass) {
		return beanClass.getName();
	}

}
