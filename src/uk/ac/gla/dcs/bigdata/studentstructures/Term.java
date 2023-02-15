package uk.ac.gla.dcs.bigdata.studentstructures;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

/**
 * @Description Token Entity
 * @Author Xiaohui Yu
 * @Date 2023/2/8
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Term implements Comparable<Term>{

    private String newsArticleId;
    private NewsArticle newsArticle;
    private long termTotalInDoc;
    private Query query;
    private String term;
    private long termNum;
    private long termNumInDoc;
    private long termNumInCorpus;
    private double DPHScore;

    public Term(String newsArticleId, NewsArticle newsArticle, long termTotalInDoc, String term) {
        this.newsArticleId = newsArticleId;
        this.newsArticle = newsArticle;
        this.termTotalInDoc = termTotalInDoc;
        this.term = term;
        this.termNum = 1;
        this.termNumInDoc = 0;
        this.termNumInCorpus = 0;
    }

    public void updateTermNumInDoc(long termNum) {
        termNumInDoc += (this.termNum + termNum);
    }

    public void updateTermInNumCorpus(long termNumInDoc) {
        termNumInCorpus += (this.termNumInDoc + termNumInDoc);
    }

    @Override
    public int compareTo(Term term) {
        return Double.compare(this.getDPHScore(), term.getDPHScore());
    }
}
