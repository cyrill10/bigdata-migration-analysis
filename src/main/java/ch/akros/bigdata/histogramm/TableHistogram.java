package ch.akros.bigdata.histogramm;

import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
public class TableHistogram implements Serializable {

    private List<ColumnHistogram> columns;

    private String tableName;

    public TableHistogram(String tableName) {
        this.tableName = tableName;
        this.columns = new ArrayList<>();
    }

    public void addColumn(Dataset<Row> dataset) {
        Map<String, String> columnMap = dataset.collectAsList().stream().collect(Collectors.toMap(row -> row.isNullAt(0) ? "null" : row.get(0).toString().replace(".", "_"), row -> String.valueOf(row.get(1))));
        ColumnHistogram columnHistogram = ColumnHistogram.builder()
                .name(dataset.columns()[0])
                .values(columnMap)
                .build();
        this.columns.add(columnHistogram);
    }

}
