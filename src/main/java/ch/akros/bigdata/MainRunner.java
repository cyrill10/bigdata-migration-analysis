package ch.akros.bigdata;

import ch.akros.bigdata.properties.SourceDatabaseProperties;
import ch.akros.bigdata.properties.SparkProperties;
import ch.akros.bigdata.properties.TargetDatabaseProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;

@Component
public class MainRunner implements CommandLineRunner {

    @Autowired
    private SourceDatabaseProperties sourceDatabaseProperties;

    @Autowired
    private TargetDatabaseProperties targetDatabaseProperties;

    @Autowired
    private SparkProperties sparkProperties;

    @Override
    public void run(String... args) throws Exception {
        URL res = MainRunner.class.getClassLoader().getResource(sourceDatabaseProperties.getDriverName());
        if (res != null) {
            File file = Paths.get(res.toURI()).toFile();
            String path = file.getCanonicalPath();

            SparkConf sparkConf = new SparkConf()
                    .setAppName("Migrationsanalyse")
                    .setMaster(sparkProperties.getMaster())
                    .set("spark.jars", path);

            SparkSession spark = SparkSession.builder()
                    .config(sparkConf)
                    .getOrCreate();

            Dataset<Row> tables = spark.read()
                    .format(sourceDatabaseProperties.getFormat())
                    .option("url", sourceDatabaseProperties.getUrl())
                    .option("query", "SELECT * FROM information_schema.tables WHERE table_schema = '" + sourceDatabaseProperties.getSchemaName() + "'")
                    .option("user", sourceDatabaseProperties.getUser())
                    .option("password", sourceDatabaseProperties.getPassword())
                    .load();

            tables.show();

            tables.toJavaRDD().collect().forEach(table -> {
                        Dataset<Row> columns = spark.read()
                                .format(sourceDatabaseProperties.getFormat())
                                .option("url", sourceDatabaseProperties.getUrl())
                                .option("dbtable", sourceDatabaseProperties.getSchemaName() + "." + table.getAs("TABLE_NAME"))
                                .option("user", sourceDatabaseProperties.getUser())
                                .option("password", sourceDatabaseProperties.getPassword())
                                .load();

                        //TODO analyze my columns and write out my analysis

                        columns.write()
                                .mode(SaveMode.Overwrite)
                                .format(targetDatabaseProperties.getFormat())
                                .option("url", targetDatabaseProperties.getUrl())
                                .option("dbtable", targetDatabaseProperties.getSchemaName() + "." + table.getAs("TABLE_NAME"))
                                .option("user", targetDatabaseProperties.getUser())
                                .option("password", targetDatabaseProperties.getPassword())
                                .save();
                    }
            );

            tables.write()
                    .mode(SaveMode.Overwrite)
                    .format(targetDatabaseProperties.getFormat())
                    .option("url", targetDatabaseProperties.getUrl())
                    .option("dbtable", targetDatabaseProperties.getSchemaName() + ".copiedTables")
                    .option("user", targetDatabaseProperties.getUser())
                    .option("password", targetDatabaseProperties.getPassword())
                    .save();

            spark.stop();
        }
    }

    // TODO use some sql to compare both tables and see if they are equal
    //    SELECT * FROM FirstTable
    //    UNION
    //    SELECT * FROM SecondTable
    //    EXCEPT
    //    SELECT * FROM FirstTable
    //    INTERSECT
    //    SELECT * FROM SecondTable;
}

