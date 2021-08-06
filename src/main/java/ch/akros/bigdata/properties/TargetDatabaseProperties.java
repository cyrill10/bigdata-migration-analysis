package ch.akros.bigdata.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Collections;
import java.util.List;

@ConfigurationProperties("database.target")
@Data
public class TargetDatabaseProperties {

    private String driverPackage;
    private String driverName;
    private String url;
    private String user;
    private String password;
    private String format;
    private String schemaName;

    public List<String> getResources() {
        return Collections.singletonList(driverPackage);
    }

}
