package ch.akros.bigdata;

import ch.akros.bigdata.spark.SparkMigrationController;
import ch.akros.bigdata.spark.SparkVerifyController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

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
}

