package step.framework.server.tables.service;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use= JsonTypeInfo.Id.CLASS,property="type")
public abstract class TableParameters {
}
