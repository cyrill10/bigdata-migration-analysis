package ch.akros.bigdata.dto.histogramm;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ColumnHistogram {

    private String columnName;

    List<EntryHistogram> entryHistograms;
}
