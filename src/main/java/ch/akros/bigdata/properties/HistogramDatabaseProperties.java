package ch.akros.bigdata.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("database.histogram")
@Data
public class HistogramDatabaseProperties {

    private String url;
    private String database;

}
