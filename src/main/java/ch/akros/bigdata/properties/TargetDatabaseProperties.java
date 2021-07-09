package ch.akros.bigdata.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("database.target")
@Data
public class TargetDatabaseProperties {

    private String driverName;
    private String url;
    private String user;
    private String password;
    private String format;
    private String schemaName;

}
