package ch.akros.bigdata.properties;

import ch.akros.bigdata.MainRunner;
import com.sun.tools.javac.util.List;
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

    public String getResources() {
        return MainRunner.getResourcesAsString(List.of(driverName));
    }

}
