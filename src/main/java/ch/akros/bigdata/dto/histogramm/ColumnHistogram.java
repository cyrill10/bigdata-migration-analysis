package ch.akros.bigdata.dto.histogramm;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.stream.Collectors;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ColumnHistogram {

    private String columnName;

    List<EntryHistogram> entryHistograms;

    public static ColumnHistogram createColumnHistogram(Dataset<Row> dataset) {
        List<EntryHistogram> entryHistograms = dataset.collectAsList().stream().map(row -> EntryHistogram.builder()
                .value(row.isNullAt(0) ? "null" : row.get(0).toString())
                .count(String.valueOf(row.get(1)))
                .build()).collect(Collectors.toList());
        return ColumnHistogram.builder()
                .columnName(dataset.columns()[0])
                .entryHistograms(entryHistograms)
                .build();
    }
}
