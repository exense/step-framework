package step.framework.server;

import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.resource.ResourceFactory;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

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

    @Test
    public void symbolicLinksAreRejected() throws IOException {
        Path root = Files.createTempDirectory("uri-spelling-alias-check");
        Path targetDirectory = root.resolve("target");
        Path targetFile = targetDirectory.resolve("secret.txt");
        Path fileLink = root.resolve("file-link.txt");
        Path directoryLink = root.resolve("directory-link");
        try {
            Files.createDirectory(targetDirectory);
            Files.writeString(targetFile, "secret");
            createSymbolicLinkOrSkip(fileLink, targetFile);
            createSymbolicLinkOrSkip(directoryLink, targetDirectory);

            ResourceFactory resourceFactory = ResourceFactory.root();
            UriSpellingAliasCheck aliasCheck = new UriSpellingAliasCheck();

            Resource fileLinkResource = resourceFactory.newResource(fileLink);
            assertTrue(fileLinkResource.isAlias());
            assertFalse(aliasCheck.checkAlias("/file-link.txt", fileLinkResource));

            Resource directoryLinkResource = resourceFactory.newResource(directoryLink);
            assertTrue(directoryLinkResource.isAlias());
            assertFalse(aliasCheck.checkAlias("/directory-link", directoryLinkResource));

            Resource combinedResource = ResourceFactory.combine(List.of(resourceFactory.newResource(targetDirectory), directoryLinkResource));
            assertFalse(aliasCheck.checkAlias("/", combinedResource));
        } finally {
            // Links first, so that deleting them never touches their targets
            for (Path path : List.of(fileLink, directoryLink, targetFile, targetDirectory, root)) {
                Files.deleteIfExists(path);
            }
        }
    }

    private static void createSymbolicLinkOrSkip(Path link, Path target) {
        try {
            Files.createSymbolicLink(link, target);
        } catch (IOException | UnsupportedOperationException e) {
            // e.g. Windows without the privilege to create symbolic links
            Assume.assumeNoException("Symbolic links cannot be created on this system", e);
        }
    }
}
