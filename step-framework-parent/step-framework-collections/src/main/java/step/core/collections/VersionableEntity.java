package step.core.collections;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import step.core.accessors.AbstractIdentifiableObject;
import step.core.accessors.AbstractOrganizableObject;

public class VersionableEntity<T extends AbstractIdentifiableObject> extends AbstractOrganizableObject {

	public static String VERSION_CUSTOM_FIELD = "versionId";
	public static long VERSION_BULK_TIME_MS = 60000;

	protected long updateTime;
	@JsonTypeInfo(use= JsonTypeInfo.Id.CLASS,property="type")
	protected T entity;

	public VersionableEntity() {
	}

	public long getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(long updateTime) {
		this.updateTime = updateTime;
	}

	public T getEntity() {
		return entity;
	}

	public void setEntity(T entity) {
		this.entity = entity;
	}

}