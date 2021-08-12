package ch.akros.bigdata.control.spark;

import ch.akros.bigdata.control.AbstractController;
import ch.akros.bigdata.properties.SparkProperties;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class SparkController extends AbstractController {

    @Autowired
    protected SparkProperties sparkProperties;
}
