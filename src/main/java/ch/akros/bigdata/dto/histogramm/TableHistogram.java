package ch.akros.bigdata.dto.histogramm;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
public class TableHistogram implements Serializable {

    private String tableName;

    private List<ColumnHistogram> columns;

    public TableHistogram(String tableName) {
        this.tableName = tableName;
        this.columns = new ArrayList<>();
    }

    public void addColumn(Dataset<Row> dataset) {
        List<EntryHistogram> entryHistograms = dataset.collectAsList().stream().map(row -> EntryHistogram.builder()
                .value(row.isNullAt(0) ? "null" : row.get(0).toString())
                .count(String.valueOf(row.get(1)))
                .build()).collect(Collectors.toList());
        ColumnHistogram columnHistogram = ColumnHistogram.builder()
                .columnName(dataset.columns()[0])
                .entryHistograms(entryHistograms)
                .build();
        this.columns.add(columnHistogram);
    }

}
