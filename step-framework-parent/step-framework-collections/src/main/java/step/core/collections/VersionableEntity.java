package step.core.collections;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import step.core.accessors.AbstractIdentifiableObject;

public class VersionableEntity<T extends AbstractIdentifiableObject> extends AbstractIdentifiableObject  {

	public static String VERSION_CUSTOM_FIELD = "versionId";

	protected long updateTime;
	@JsonTypeInfo(use= JsonTypeInfo.Id.CLASS,property="type")
	protected T entity;

	public VersionableEntity() {
	}

	public VersionableEntity(T entity) {
		this.updateTime = System.currentTimeMillis();
		this.entity = entity;
		this.entity.addCustomField(VERSION_CUSTOM_FIELD, this.getId().toHexString());
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
