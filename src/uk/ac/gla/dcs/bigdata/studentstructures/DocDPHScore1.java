package uk.ac.gla.dcs.bigdata.studentstructures;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;

/**
 * @Description Document DPH score structure
 * @Author Xiaohui Yu
 * @Date 2023/2/15
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DocDPHScore1 {

    private String newsArticleId;
    private NewsArticle newsArticle;
    private String originalQuery;
    private Query query;
    private double DPHScore;

    public RankedResult getRankedResult() {
        return new RankedResult(newsArticleId, getNewsArticle(), DPHScore);
    }

    public NewsArticle getNewsArticle() {
        return newsArticle;
        // return new NewsArticle(newsArticleId, article_url, title, author, published_date, contents, type, source);
    }

    public String getTitle() {
        return newsArticle.getTitle();
    }

    public Query getQuery() {
        return query;
        // return new Query(originalQuery, queryTerms, queryTermCounts);
    }
}
