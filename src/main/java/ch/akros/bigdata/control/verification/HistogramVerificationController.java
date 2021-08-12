package ch.akros.bigdata.control.verification;

import ch.akros.bigdata.control.spark.SparkController;
import ch.akros.bigdata.dto.histogramm.ColumnHistogram;
import ch.akros.bigdata.dto.histogramm.TableHistogram;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Component
public class HistogramVerificationController extends SparkController {

    private static final Logger logger = LoggerFactory.getLogger(HistogramVerificationController.class);

    public void verifyHistogramCreation() {

        ConnectionString connectionString = new ConnectionString(histogramDatabaseProperties.getUrl());

        CodecRegistry pojoCodecRegistry = fromProviders(PojoCodecProvider.builder().automatic(true).build());

        CodecRegistry codecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                pojoCodecRegistry);

        MongoClientSettings clientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .codecRegistry(codecRegistry)
                .build();


        try (Connection source = DriverManager.getConnection(sourceDatabaseProperties.getUrl(), sourceDatabaseProperties
                .getUser(), sourceDatabaseProperties.getPassword());
             MongoClient targertClient = MongoClients.create(clientSettings)) {

            MongoDatabase target = targertClient.getDatabase(histogramDatabaseProperties.getDatabase());

            MongoCollection<TableHistogram> histograms = target.getCollection(histogramDatabaseProperties.getCollection(), TableHistogram.class);

            List<String> problems = new ArrayList<>();

            try (PreparedStatement table_stmt = source.prepareStatement("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '" + sourceDatabaseProperties
                    .getSchemaName() + "'");
                 ResultSet tableSet = table_stmt.executeQuery()) {
                while (tableSet.next()) {

                    String tableName = tableSet.getString(1);

                    Bson filter = Filters.eq("tableName", tableName);

                    TableHistogram histogram = histograms.find(filter).first();

                    if (histogram != null) {
                        try (PreparedStatement columns_stmt = source.
                                prepareStatement("SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '" + sourceDatabaseProperties
                                        .getSchemaName() + "' AND TABLE_NAME = '" + tableName + "'");
                             ResultSet columnsSet = columns_stmt.executeQuery()) {

                            while (columnsSet.next()) {
                                String columnName = columnsSet.getString(1);

                                String dataType = columnsSet.getString(2);


                                Optional<ColumnHistogram> columnHistogram = histogram.getColumns()
                                        .stream()
                                        .filter(column -> column.getColumnName().equals(columnName))
                                        .findFirst();

                                if (columnHistogram.isPresent()) {
                                    AtomicInteger totalNumberOfEntries = new AtomicInteger();
                                    columnHistogram.get().getEntryHistograms().forEach(entryHistogram -> {
                                        totalNumberOfEntries.addAndGet(Integer.parseInt(entryHistogram.getCount()));

                                        try (PreparedStatement source_entryCount_stmt = source.
                                                prepareStatement("SELECT count(*) FROM " + sourceDatabaseProperties.getSchemaName() + "." + histogram
                                                        .getTableName() +
                                                        " WHERE " + getComparingString(columnName, entryHistogram.getValue(), dataType));
                                             ResultSet entryCountSet = source_entryCount_stmt.executeQuery()) {
                                            while (entryCountSet.next()) {
                                                String sourceCount = entryCountSet.getString(1);

                                                if (!sourceCount.equals(entryHistogram.getCount())) {
                                                    problems.add("Value " + entryHistogram.getValue() + " in " + tableName + "." + columnName + " is counted as: " + entryHistogram
                                                            .getCount() + ", but should be: " + sourceCount);
                                                }
                                            }
                                        } catch (SQLException e) {
                                            e.printStackTrace();
                                        }
                                    });

                                    try (PreparedStatement source_totalEntryCount_stmt = source.
                                            prepareStatement("SELECT count(*) FROM " + sourceDatabaseProperties.getSchemaName() + "." + histogram
                                                    .getTableName());
                                         ResultSet totalEntryCountSet = source_totalEntryCount_stmt.executeQuery()) {

                                        while (totalEntryCountSet.next()) {
                                            int sourceTotalCount = totalEntryCountSet.getInt(1);

                                            if (sourceTotalCount != totalNumberOfEntries.get()) {
                                                problems.add("Histogram does not have all Entrys for: " + tableName + "." + columnName);
                                            }
                                        }
                                    }
                                } else {
                                    problems.add("No histogram found for column: " + columnName);
                                }
                            }
                        }
                    } else {
                        problems.add("No histogram found for table: " + tableName);
                    }
                }
            }

            if (!problems.isEmpty()) {
                throw new RuntimeException("not identical: " + problems.toString());
            }

        } catch (
                SQLException e) {
            e.printStackTrace();
        }

        logger.warn("Verifying Histogram successful");
    }

    private String getComparingString(String columnName, String value, String dataType) {
        if (value.equals("null")) {
            return columnName + " is null";
        } else if (dataType.equals("datetime")) {
            return columnName + " = str_to_date('" + value + "', '%Y-%m-%d %H:%i:%s.%f')";
        } else if (dataType.equals("float")) {
            if (value.endsWith(".0")) {
                return columnName + " LIKE '" + value.substring(0, value.length() - 2) + "'";
            }
            return columnName + " LIKE '" + value + "'";
        } else if (dataType.equals("bit")) {
            return columnName + " = " + value;
        }
        return "BINARY " + columnName + " = '" + value + "'";
    }
}
