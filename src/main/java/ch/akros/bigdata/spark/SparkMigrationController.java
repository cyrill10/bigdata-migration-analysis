package ch.akros.bigdata.spark;

import ch.akros.bigdata.histogramm.TableHistogram;
import com.mongodb.spark.MongoSpark;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class SparkMigrationController extends SparkController {

    private static Logger logger = LoggerFactory.getLogger(SparkMigrationController.class);

    public void runSparkCopy() {
        ArrayList<String> jars = new ArrayList<>();
        jars.add(sourceDatabaseProperties.getResources());
        jars.add(histogramDatabaseProperties.getResources());

        SparkConf sparkConf = new SparkConf()
                .setAppName("Migrationsanalyse")
                .setMaster(sparkProperties.getMaster())
                .set("spark.ui.enabled", "false")
                .set("spark.mongodb.output.uri", histogramDatabaseProperties.getUrl())
                .set("spark.mongodb.output.database", histogramDatabaseProperties.getDatabase())
                .set("spark.mongodb.output.collection", "dummy")
                .set("spark.jars", jars.stream().reduce("", (s1, s2) -> s1 + "," + s2));

        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        Dataset<Row> tables_defintions = spark.read()
                .format(sourceDatabaseProperties.getFormat())
                .option("url", sourceDatabaseProperties.getUrl())
                //  switch between all and only one table
//                .option("query", "SELECT * FROM information_schema.tables WHERE table_schema = '" + sourceDatabaseProperties.getSchemaName() + "' and TABLE_NAME = 'real_account'")
                .option("query", "SELECT * FROM information_schema.tables WHERE table_schema = '" + sourceDatabaseProperties.getSchemaName() + "'")
                .option("user", sourceDatabaseProperties.getUser())
                .option("password", sourceDatabaseProperties.getPassword())
                .load();

        tables_defintions.show();


        tables_defintions.toJavaRDD().collect().forEach(table_definition -> {
                    String tableName = table_definition.getAs("TABLE_NAME");
                    Dataset<Row> table = spark.read()
                            .format(sourceDatabaseProperties.getFormat())
                            .option("url", sourceDatabaseProperties.getUrl())
                            .option("dbtable", sourceDatabaseProperties.getSchemaName() + "." + tableName)
                            .option("user", sourceDatabaseProperties.getUser())
                            .option("password", sourceDatabaseProperties.getPassword())
                            .load();


                    TableHistogram tableH = new TableHistogram(tableName);

                    // Calcualte Histogramm for each column
                    List<Dataset<Row>> histogramsForTable = Arrays.stream(table.dtypes()).map(columnType -> table.groupBy(columnType._1)).map(RelationalGroupedDataset::count).collect(Collectors.toList());

                    // should be foreach loop
                    histogramsForTable.forEach(tableH::addColumn);

                    logger.warn(tableH.toString());
                    Encoder<TableHistogram> encoder = Encoders.bean(TableHistogram.class);
                    Dataset<TableHistogram> histogramFrame = spark.createDataset(Collections.singletonList(tableH), encoder);
                    MongoSpark.write(histogramFrame).option("collection", tableName).mode(SaveMode.Overwrite).save();

                    table.write()
                            .mode(SaveMode.Overwrite)
                            .format(targetDatabaseProperties.getFormat())
                            .option("url", targetDatabaseProperties.getUrl())
                            .option("dbtable", targetDatabaseProperties.getSchemaName() + "." + tableName)
                            .option("user", targetDatabaseProperties.getUser())
                            .option("password", targetDatabaseProperties.getPassword())
                            .save();
                }
        );

        tables_defintions.write()
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
