package step.framework.server.tables.service;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import step.core.collections.Filter;

@JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
@JsonSubTypes({@JsonSubTypes.Type(FieldFilter.class), @JsonSubTypes.Type(FulltextFilter.class), @JsonSubTypes.Type(OQLFilter.class)})
public abstract class TableFilter {

    public abstract Filter toFilter();

}
