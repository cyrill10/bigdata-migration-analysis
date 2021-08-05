package ch.akros.bigdata;

import ch.akros.bigdata.spark.SparkMigrationController;
import ch.akros.bigdata.spark.SparkVerifyController;
import com.sun.tools.javac.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Objects;

@Component
public class MainRunner implements CommandLineRunner {

    @Autowired
    SparkMigrationController sparkMigrationController;

    @Autowired
    SparkVerifyController sparkVerifyController;

    @Override
    public void run(String... args) {
        sparkMigrationController.runSparkCopy();
//        sparkVerifyController.verifySparkCopy();
    }

    public static String getResourcesAsString(List<String> resources) {
        return resources.stream().map(MainRunner.class.getClassLoader()::getResource).filter(Objects::nonNull).map(url -> {
                    try {
                        return Paths.get(url.toURI()).toFile().getCanonicalPath();
                    } catch (IOException | URISyntaxException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
        ).reduce("", (s1, s2) -> s1 + "," + s2);
    }


}

