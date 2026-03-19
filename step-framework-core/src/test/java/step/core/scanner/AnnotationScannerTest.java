/*******************************************************************************
 * Copyright (C) 2020, exense GmbH
 *
 * This file is part of STEP
 *
 * STEP is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * STEP is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with STEP.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package step.core.scanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.io.Files;

import ch.exense.commons.io.FileHelper;
//import step.core.dynamicbeans.ContainsDynamicValues;

public class AnnotationScannerTest {

    @Test
    public void test() {
        try (AnnotationScanner annotationScanner = AnnotationScanner.forAllClassesFromContextClassLoader()) {
            Class<?> class1 = annotationScanner.getClassesWithAnnotation((TestAnnotation.class)).stream().findFirst().get();
            assertEquals(AnnotatedClass.class, class1);

            Method method1 = annotationScanner.getMethodsWithAnnotation(TestAnnotation.class).stream().findFirst().get();
            assertEquals("testMethod", method1.getName());
        }
    }

    @Test
    public void test2() {
        try (AnnotationScanner annotationScanner = AnnotationScanner.forAllClassesFromClassLoader(this.getClass().getClassLoader())) {
            Class<?> class1 = annotationScanner.getClassesWithAnnotation((TestAnnotation.class)).stream().findFirst().get();
            assertEquals(AnnotatedClass.class, class1);

            Method method1 = annotationScanner.getMethodsWithAnnotation(TestAnnotation.class).stream().findFirst().get();
            assertEquals("testMethod", method1.getName());
        }
    }

    @Test
    public void test3() {
        try (AnnotationScanner annotationScanner = AnnotationScanner.forAllClassesFromClassLoader("step", this.getClass().getClassLoader())) {
            Class<?> class1 = annotationScanner.getClassesWithAnnotation((TestAnnotation.class)).stream().findFirst().get();
            assertEquals(AnnotatedClass.class, class1);

            Method method1 = annotationScanner.getMethodsWithAnnotation(TestAnnotation.class).stream().findFirst().get();
            assertEquals("testMethod", method1.getName());
        }
    }

    /*jar file is created manually
     * go to the test\java folder
     * javac .\step\core\scanner\TestAnnotation.java .\step\core\scanner\AnnotatedClass.java
     * jar cf ..\resources\annotation-test.jar .\step\core\scanner\AnnotatedClass.class
     */

    @Test
    public void testAnnotationScannerForSpecificJars() {
        File file = FileHelper.getClassLoaderResourceAsFile(this.getClass().getClassLoader(), "annotation-test.jar");
        try (AnnotationScanner annotationScanner = AnnotationScanner.forSpecificJar(file)) {
            List<Method> methods = annotationScanner.getMethodsWithAnnotation(TestAnnotation.class).stream().collect(Collectors.toList());
            assertEquals(1, methods.size());
            assertEquals("testMethod", methods.get(0).getName());
        }
    }

    @Test
    public void testAnnotationScannerForSpecificJarsWithSpacesAndSpecialCharsInPath() throws IOException {
        File file = FileHelper.getClassLoaderResourceAsFile(this.getClass().getClassLoader(), "annotation-test.jar");
        File folderWithSpace = FileHelper.createTempFolder("Folder with space");
        File targetFile = new File(folderWithSpace.getAbsolutePath() + "/annotat iôn-tèst.jar");
        Files.copy(file, targetFile);
        try (AnnotationScanner annotationScanner = AnnotationScanner.forSpecificJar(targetFile)) {
            List<Method> methods = annotationScanner.getMethodsWithAnnotation(TestAnnotation.class).stream().collect(Collectors.toList());
            assertEquals(1, methods.size());
            assertEquals("testMethod", methods.get(0).getName());
        }
    }

    @Test
    public void testGetClasspathElementUrl_found() {
        File file = FileHelper.getClassLoaderResourceAsFile(this.getClass().getClassLoader(), "annotation-test.jar");
        try (AnnotationScanner annotationScanner = AnnotationScanner.forSpecificJar(file)) {
            URL url = annotationScanner.getClasspathElementUrl("step.core.scanner.AnnotatedClass");
            assertNotNull(url);
            assertEquals(file.toURI().toURL(), url);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testGetClasspathElementUrl_notFound() {
        File file = FileHelper.getClassLoaderResourceAsFile(this.getClass().getClassLoader(), "annotation-test.jar");
        try (AnnotationScanner annotationScanner = AnnotationScanner.forSpecificJar(file)) {
            URL url = annotationScanner.getClasspathElementUrl("com.example.NonExistentClass");
            assertNull(url);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetClasspathElementUrl_nullClassName() {
        try (AnnotationScanner annotationScanner = AnnotationScanner.forAllClassesFromContextClassLoader()) {
            annotationScanner.getClasspathElementUrl(null);
        }
    }

    @Test
    public void testGetClasspathElementUrl_multipleJars_returnsCorrectJar() throws Exception {
        File annotationTestJar = FileHelper.getClassLoaderResourceAsFile(this.getClass().getClassLoader(), "annotation-test.jar");
        // Use the classgraph JAR (already a compile dependency) as a second distinct JAR
        URL classgraphJarUrl = io.github.classgraph.ClassGraph.class.getProtectionDomain().getCodeSource().getLocation();
        File classgraphJar = new File(classgraphJarUrl.toURI());

        URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{
                annotationTestJar.toURI().toURL(),
                classgraphJarUrl
        }, null);

        try (AnnotationScanner annotationScanner = AnnotationScanner.forSpecificJarFromURLClassLoader(urlClassLoader)) {
            URL annotatedClassUrl = annotationScanner.getClasspathElementUrl("step.core.scanner.AnnotatedClass");
            assertNotNull(annotatedClassUrl);
            assertEquals(annotationTestJar.getCanonicalFile(), new File(annotatedClassUrl.toURI()).getCanonicalFile());

            URL classgraphClassUrl = annotationScanner.getClasspathElementUrl("io.github.classgraph.ClassGraph");
            assertNotNull(classgraphClassUrl);
            assertEquals(classgraphJar.getCanonicalFile(), new File(classgraphClassUrl.toURI()).getCanonicalFile());
        }
    }

    // Don't remove this class
    // It is here to ensure that annotation scanning performed in
    // testGetMethodsWithAnnotation() isn't finding other methods that the
    // one contained in the specified jar step-core-model-test.jar
    public static class TestBean {

        //@ContainsDynamicValues
        public void testMethod2() {
            // This method
        }

    }

}
