package step.framework.server.tables.service;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import step.core.collections.Filter;

@JsonTypeInfo(use= JsonTypeInfo.Id.DEDUCTION)
@JsonSubTypes({@JsonSubTypes.Type(FieldFilter.class), @JsonSubTypes.Type(FulltextFilter.class)})
public abstract class TableFilter {

    abstract Filter toFilter();

}
