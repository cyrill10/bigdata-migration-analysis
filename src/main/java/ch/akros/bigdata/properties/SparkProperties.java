package ch.akros.bigdata.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("spark")
@Data
public class SparkProperties {
    private String master;

    private String driverHost;

    private String bindAdress;

    private String driverPort;

    private String blockManagerPort;
}
