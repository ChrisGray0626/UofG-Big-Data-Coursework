package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.spark.api.java.function.MapGroupsFunction;
import uk.ac.gla.dcs.bigdata.constant.ValueConstant;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentstructures.Term;

/**
 * @Description
 * @Author Chris
 * @Date 2023/2/20
 */
public class TermToDocumentRanking implements MapGroupsFunction<String, Term, DocumentRanking> {

    @Override
    public DocumentRanking call(String key, Iterator<Term> termIterator) throws Exception {
        List<Term> terms = IteratorUtils.toList(termIterator);
        // Order the terms by the DPH score in descending order
        terms.sort(Comparator.comparing(Term::getDPHScore).reversed());
        List<RankedResult> rankedResults = new ArrayList<>();
        Query query = terms.get(0).getQuery();
        for (Term term : terms) {
            // Add the item if the similarity is less than the threshold
            boolean isSimilar = false;
            for (RankedResult rankedResult : rankedResults) {
                double similarity =
                        TextDistanceCalculator.similarity(rankedResult.getArticle().getTitle(), term.getNewsArticle().getTitle());
                if (similarity < ValueConstant.SIMILARITY_THRESHOLD) {
                    isSimilar = true;
                }
            }
            if (isSimilar) {
                continue;
            }
            rankedResults.add(new RankedResult(term.getNewsArticleId(), term.getNewsArticle(), term.getDPHScore()));
            // Rank the first 10 items
            if (rankedResults.size() >= ValueConstant.RANKING_SIZE) {
                break;
            }
        }

        return new DocumentRanking(query, rankedResults);
    }
}
