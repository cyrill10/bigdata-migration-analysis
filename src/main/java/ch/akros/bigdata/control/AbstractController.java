package ch.akros.bigdata.control;

import ch.akros.bigdata.properties.HistogramDatabaseProperties;
import ch.akros.bigdata.properties.SourceDatabaseProperties;
import ch.akros.bigdata.properties.TargetDatabaseProperties;
import org.springframework.beans.factory.annotation.Autowired;

public class AbstractController {

    @Autowired
    protected SourceDatabaseProperties sourceDatabaseProperties;

    @Autowired
    protected TargetDatabaseProperties targetDatabaseProperties;

    @Autowired
    protected HistogramDatabaseProperties histogramDatabaseProperties;

}
