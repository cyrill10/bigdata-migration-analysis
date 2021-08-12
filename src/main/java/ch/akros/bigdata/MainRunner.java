package ch.akros.bigdata;

import ch.akros.bigdata.control.spark.SparkMigrationController;
import ch.akros.bigdata.control.verification.HistogramVerificationController;
import ch.akros.bigdata.control.verification.MigrationVerificationController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class MainRunner implements CommandLineRunner {

    @Autowired
    SparkMigrationController sparkMigrationController;

    @Autowired
    MigrationVerificationController sparkVerifyController;

    @Autowired
    HistogramVerificationController histogramVerificationController;

    @Override
    public void run(String... args) {
        sparkMigrationController.runSparkCopy();
        sparkVerifyController.verifySparkCopy();
        histogramVerificationController.verifyHistogramCreation();
    }
}

