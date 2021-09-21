package ch.akros.bigdata.control.verification;

import ch.akros.bigdata.control.spark.SparkController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class MigrationVerificationController extends SparkController {

    private static final int NANOSECONDS_IN_SECOND = 1000000000;


    private static final Logger logger = LoggerFactory.getLogger(MigrationVerificationController.class);

    public void verifySparkCopy() {

        List<String> missMatches = new ArrayList<>();

        try (Connection source = DriverManager.getConnection(sourceDatabaseProperties.getUrl(), sourceDatabaseProperties
                .getUser(), sourceDatabaseProperties.getPassword());
             Connection target = DriverManager.getConnection(targetDatabaseProperties.getUrl(), targetDatabaseProperties
                     .getUser(), targetDatabaseProperties.getPassword())) {

            try (PreparedStatement table_stmt = source.prepareStatement("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = '" + sourceDatabaseProperties
                    .getSchemaName() + "'");
                 ResultSet tableSet = table_stmt.executeQuery()) {

                while (tableSet.next()) {

                    Map<Long, String> sourceIdHashMap = new HashMap<>();
                    Map<Long, String> targetIdHashMap = new HashMap<>();

                    try (PreparedStatement source_content_stmt = source.prepareStatement("SELECT * FROM " + sourceDatabaseProperties
                                    .getSchemaName() + "." + tableSet.getString(1),
                            ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY);

                         PreparedStatement target_content_stmt = target.prepareStatement("SELECT * FROM " + targetDatabaseProperties
                                         .getSchemaName() + "." + tableSet.getString(1),
                                 ResultSet.TYPE_FORWARD_ONLY,
                                 ResultSet.CONCUR_READ_ONLY);

                         ResultSet sourceResultSet = source_content_stmt.executeQuery();
                         ResultSet targetResultSet = target_content_stmt.executeQuery()) {

                        do {
                            if (sourceResultSet.next()) {
                                if (targetResultSet.next()) {
                                    // Compare the lines
                                    long sourceHash = hash(getRowValues(sourceResultSet, sourceResultSet.getMetaData()));
                                    long targetHash = hash(getRowValues(targetResultSet, targetResultSet.getMetaData()));

                                    sourceIdHashMap.put(sourceHash, sourceResultSet.getString(1));
                                    targetIdHashMap.put(targetHash, targetResultSet.getString(1));

                                    if (targetIdHashMap.containsKey(sourceHash)) {
                                        targetIdHashMap.remove(sourceHash);
                                        sourceIdHashMap.remove(sourceHash);
                                    }
                                    if (sourceIdHashMap.containsKey(targetHash)) {
                                        sourceIdHashMap.remove(targetHash);
                                        targetIdHashMap.remove(targetHash);
                                    }
                                } else {
                                    // Add the source row
                                    long sourceHash = hash(getRowValues(sourceResultSet, sourceResultSet.getMetaData()));
                                    sourceIdHashMap.put(sourceHash, sourceResultSet.getString(1));
                                }
                            } else {
                                if (targetResultSet.next()) {
                                    // Add the target row
                                    long targetHash = hash(getRowValues(targetResultSet, targetResultSet.getMetaData()));
                                    targetIdHashMap.put(targetHash, targetResultSet.getString(1));
                                } else {
                                    break;
                                }
                            }
                        } while (true);
                    }

                    for (Map.Entry<Long, String> mapEntry : sourceIdHashMap.entrySet()) {
                        if (targetIdHashMap.containsKey(mapEntry.getKey())) {
                            targetIdHashMap.remove(mapEntry.getKey());
                            continue;
                        }
                        missMatches.add(sourceDatabaseProperties.getSchemaName() + "." + tableSet.getString(1) + ": " + mapEntry
                                .getValue());
                    }
                    for (Map.Entry<Long, String> mapEntry : targetIdHashMap.entrySet()) {
                        if (sourceIdHashMap.containsKey(mapEntry.getKey())) {
                            sourceIdHashMap.remove(mapEntry.getKey());
                            continue;
                        }
                        missMatches.add(targetDatabaseProperties.getSchemaName() + "." + tableSet.getString(1) + ": " + mapEntry
                                .getValue());
                    }
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (!missMatches.isEmpty()) {
            throw new RuntimeException("not identical: " + missMatches.toString());
        }

        logger.warn("Verifying Migration successful");
    }

    private Object[] getRowValues(ResultSet resultSet, ResultSetMetaData resultSetMetaData) throws SQLException {
        List<Object> rowValues = new ArrayList<>();
        for (int i = 2; i < resultSetMetaData.getColumnCount(); i++) {
            if (LocalDateTime.class.getName().equalsIgnoreCase(resultSetMetaData.getColumnClassName(i)) ||
                    Timestamp.class.getName().equalsIgnoreCase(resultSetMetaData.getColumnClassName(i))) {
                Timestamp timestamp = resultSet.getTimestamp(i);
                if (timestamp != null) {
                    rowValues.add(roundedTimestamp(timestamp.toLocalDateTime()));
                } else {
                    rowValues.add(timestamp);
                }
            } else {
                rowValues.add(resultSet.getObject(i));
            }
        }
        return rowValues.toArray(new Object[0]);
    }

    private LocalDateTime roundedTimestamp(LocalDateTime timestamp) {
        if (timestamp.getNano() > (NANOSECONDS_IN_SECOND / 2)) {
            return timestamp.withNano(0).plusSeconds(1);
        }
        return timestamp.withNano(0);
    }


    private Long hash(Object... objects) {
        StringBuilder builder = new StringBuilder();
        for (Object object : objects) {
            builder.append(object);
        }
        return hash(builder.toString());
    }

    private Long hash(String string) {
        // Must be prime of course
        long seed = 131; // 31 131 1313 13131 131313 etc..
        long hash = 0;
        char[] chars = string.toCharArray();
        for (char aChar : chars) {
            hash = (hash * seed) + aChar;
        }
        return Math.abs(hash);
    }

}
