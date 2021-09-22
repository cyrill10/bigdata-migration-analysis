package ch.akros.bigdata;

import ch.akros.bigdata.control.spark.SparkMigrationController;
import ch.akros.bigdata.control.verification.HistogramVerificationController;
import ch.akros.bigdata.control.verification.MigrationVerificationController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class MainRunner implements CommandLineRunner {

    @Autowired
    SparkMigrationController sparkMigrationController;

    @Autowired
    MigrationVerificationController sparkVerifyController;

    @Autowired
    HistogramVerificationController histogramVerificationController;

    private static final Logger logger = LoggerFactory.getLogger(SparkMigrationController.class);

    @Override
    public void run(String... args) {

        long startTime = System.currentTimeMillis();

        logger.warn("Start at process at: " + LocalDateTime.now());

        sparkMigrationController.runSparkCopy();

        long endOfMigartion = System.currentTimeMillis();

        logger.warn("Migration took: " + (endOfMigartion - startTime) / 1000 + " Seconds");

        // verify
        sparkVerifyController.verifySparkCopy();

        long endOfCopyVerification = System.currentTimeMillis();
        logger.warn("Copy Verification took: " + (endOfCopyVerification - endOfMigartion) / 1000 + " Seconds");

        histogramVerificationController.verifyHistogramCreation();

        long endOfHistogramVerification = System.currentTimeMillis();
        logger.warn("Histogram Verification took: " + (endOfHistogramVerification - endOfCopyVerification) / 1000 + " Seconds");

        logger.warn("Total Process took: " + (endOfHistogramVerification - startTime) / 1000 + " Seconds");
    }
}

