package step.framework.server.tables.service;

import step.core.collections.Filter;

public class CollectionFilter extends TableFilter {

    private Filter collectionFilter;

    public Filter getCollectionFilter() {
        return collectionFilter;
    }

    public void setCollectionFilter(Filter collectionFilter) {
        this.collectionFilter = collectionFilter;
    }

    @Override
    public Filter toFilter() {
        return collectionFilter;
    }
}
