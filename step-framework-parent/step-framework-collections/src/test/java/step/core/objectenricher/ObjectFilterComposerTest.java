package step.core.objectenricher;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class ObjectFilterComposerTest {

    @Test
    public void compose() {
        ObjectFilter objectFilter = ObjectFilterComposer.compose(List.of(() -> "key1=\"value1\"", () -> "key2=\"value2\""));
        assertEquals("key1=\"value1\" and key2=\"value2\"",objectFilter.getOQLFilter());
    }
}