package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

/**
 * @Description Query-documents score structure
 * @Author Xiaohui Yu
 * @Date 2023/2/20
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueryDocScore implements Comparable<QueryDocScore>, Serializable {

    private Query query;
    private NewsArticle newsArticle;
    private double DPHScore;

    @Override
    public int compareTo(@NotNull QueryDocScore o) {
        return Double.compare(this.getDPHScore(), o.getDPHScore());
    }
}
