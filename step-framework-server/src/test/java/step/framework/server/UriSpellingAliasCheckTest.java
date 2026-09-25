package step.framework.server;

import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UriSpellingAliasCheckTest {

    private static final String NESTED_JAR = "nested:/opt/step/step/!BOOT-INF/lib/step-ide.jar!/dist/step-ide";

    @Test
    public void nestedJarUriSpellingIsSameResource() {
        assertTrue(UriSpellingAliasCheck.isSameResource(
            URI.create("jar:nested:///opt/step/step/!BOOT-INF/lib/step-ide.jar!/dist/step-ide/index.html"),
            URI.create("jar:" + NESTED_JAR + "/index.html")));
    }

    @Test
    public void directoryTrailingSlashIsSameResource() {
        assertTrue(UriSpellingAliasCheck.isSameResource(
            URI.create("jar:nested:///opt/step/step/!BOOT-INF/lib/step-ide.jar!/dist/step-ide/"),
            URI.create("jar:" + NESTED_JAR)));
    }

    @Test
    public void differentResourceIsRejected() {
        assertFalse(UriSpellingAliasCheck.isSameResource(
            URI.create("jar:nested:///opt/step/step/!BOOT-INF/lib/step-ide.jar!/dist/step-ide/index.html"),
            URI.create("jar:" + NESTED_JAR + "/other.html")));
        // Symbolic link
        assertFalse(UriSpellingAliasCheck.isSameResource(
            URI.create("file:///var/www/link/index.html"), URI.create("file:///srv/target/index.html")));
        // Case variant
        assertFalse(UriSpellingAliasCheck.isSameResource(
            URI.create("file:///C:/www/INDEX.html"), URI.create("file:///C:/www/index.html")));
        // Unresolvable real path
        assertFalse(UriSpellingAliasCheck.isSameResource(URI.create("file:///var/www/index.html"), null));
    }
}
