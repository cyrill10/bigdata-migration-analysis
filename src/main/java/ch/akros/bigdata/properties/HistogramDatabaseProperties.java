package ch.akros.bigdata.properties;

import ch.akros.bigdata.MainRunner;
import com.sun.tools.javac.util.List;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("database.histogram")
@Data
public class HistogramDatabaseProperties {

    private String driverName;
    private String url;
    private String database;
    private String bsonDriver;
    private String coreDriver;
    private String syncDriver;

    public String getResources() {
        return MainRunner.getResourcesAsString(List.of(driverName, bsonDriver, coreDriver, syncDriver));
    }
}
