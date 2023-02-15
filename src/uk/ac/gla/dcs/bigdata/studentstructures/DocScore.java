package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

/**
 * @Description
 * @Author Chris
 * @Date 2023/2/21
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DocScore implements Serializable {

    private NewsArticle newsArticle;
    private double score;

    public boolean equals(DocScore docScore) {
        return this.newsArticle.equals(docScore.getNewsArticle());
    }

    public int hashCode() {
        return this.newsArticle.hashCode();
    }
}
