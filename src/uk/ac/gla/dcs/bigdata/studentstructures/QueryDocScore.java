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
 * @Description
 * @Author Chris
 * @Date 2023/2/20
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueryDocScore implements Comparable<QueryDocScore>, Serializable {

    // TODO Remove
    private Map<Query, List<DocScore>> results;
    private Query query;
    private NewsArticle newsArticle;
    private double DPHScore;

    public QueryDocScore(Query query, NewsArticle newsArticle,
                         double DPHScore) {
        // results = new ConcurrentHashMap<>();
        // results.put(query, new ArrayList<>(){{
        //     add(new DocScore(newsArticle, DPHScore));
        // }});
        this.query = query;
        this.newsArticle = newsArticle;
        this.DPHScore = DPHScore;
    }

    @Override
    public int compareTo(@NotNull QueryDocScore o) {
        return Double.compare(this.getDPHScore(), o.getDPHScore());
    }
}
