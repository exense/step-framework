package step.core.entities;

import step.core.accessors.AbstractOrganizableObject;

/**
 * A simple bean that doesn't extend {@link AbstractOrganizableObject}
 */
public class SimpleBean {

    private String id;
    private String stringProperty;

    public SimpleBean() {
        super();
    }

	public SimpleBean(String stringProperty) {
		this.stringProperty = stringProperty;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getStringProperty() {
		return stringProperty;
	}

	public void setStringProperty(String stringProperty) {
		this.stringProperty = stringProperty;
	}
}


