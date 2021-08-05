package ch.akros.bigdata.histogramm;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class ColumnHistogram {

    private String name;

    Map<String, String> values;
}
