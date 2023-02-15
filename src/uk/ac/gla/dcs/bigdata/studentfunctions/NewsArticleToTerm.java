package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.spark.api.java.function.FlatMapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.Term;

/**
 * @Description
 * @Author Chris
 * @Date 2023/2/20
 */
public class NewsArticleToTerm implements FlatMapFunction<NewsArticle, Term> {

    @Override
    public Iterator<Term> call(NewsArticle newsArticle) throws Exception {
        // Remove the null title
        if (newsArticle.getTitle() == null) {
            return Collections.emptyIterator();
        }
        List<String> terms = new ArrayList<>();
        List<String> texts = newsArticleToText(newsArticle);
        texts.forEach(text -> {
            TextPreProcessor processor = new TextPreProcessor();
            terms.addAll(processor.process(text));
        });
        int termTotalInDoc = terms.size();

        return terms.stream()
                .map(term -> new Term(newsArticle.getId(), newsArticle, termTotalInDoc, term))
                .collect(Collectors.toList())
                .iterator();
    }

    private List<String> newsArticleToText(NewsArticle newsArticle) {
        List<String> texts = new ArrayList<>();
        String title = newsArticle.getTitle();
        // Add the title
        texts.add(title);
        // Add the first 5 paragraph
        List<String> contents = newsArticle.getContents().stream()
                        .filter(content -> {
                            // Remove the null content
                            if (content == null)
                                return false;
                            return "paragraph".equals(content.getSubtype());
                        })
                        .limit(5)
                        .map(ContentItem::getContent)
                        .collect(Collectors.toList());
        texts.addAll(contents);

        return texts;
    }
}
