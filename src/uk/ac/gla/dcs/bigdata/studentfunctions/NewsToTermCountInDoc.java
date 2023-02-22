package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.accumulator.TermNumMapAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.TermCount;

/**
 * @Description
 * @Author Xiaohui Yu
 * @Date 2023/2/21
 */
@AllArgsConstructor
public class NewsToTermCountInDoc implements FlatMapFunction<NewsArticle, TermCount> {

    private final Broadcast<Map<String, Query>> queryTermMapBroadcast;
    private final TermNumMapAccumulator termNumMapAccumulator;
    private final LongAccumulator docTotalInCorpusAccumulator;
    private final LongAccumulator termTotalInCorpusAccumulator;

    @Override
    public Iterator<TermCount> call(NewsArticle news) throws Exception {
        List<TermCount> termCountInDocs = new ArrayList<>();
        // Convert news to texts
        List<String> terms = new ArrayList<>();
        List<String> texts = newsToText(news);
        texts.forEach(text -> {
            TextPreProcessor processor = new TextPreProcessor();
            terms.addAll(processor.process(text));
        });
        // Term total number in the document & corpus
        int termTotalInDoc = terms.size();
        termTotalInCorpusAccumulator.add(termTotalInDoc);
        // Term number in the document & corpus
        Map<String, Long> termNumInDocMap = new HashMap<>();
        Map<String, Query> queryTermMap = queryTermMapBroadcast.value();
        // Collect the query in news
        Set<Query> queries = new HashSet<>();
        terms.forEach(term -> {
            // Filter news without any query term
            if (queryTermMap.containsKey(term)) {
                termNumInDocMap.put(term, termNumInDocMap.getOrDefault(term, 0L) + 1);
                queries.add(queryTermMap.get(term));
                termNumMapAccumulator.add(term);
            }
        });
        queries.forEach(query -> termCountInDocs.add(new TermCount(news, query, termNumInDocMap, termTotalInDoc)));
        docTotalInCorpusAccumulator.add(1);
        // if (termNumInDocMap.isEmpty()) {
        //     return Collections.emptyIterator();
        // }

        return termCountInDocs.iterator();
    }

    private List<String> newsToText(NewsArticle newsArticle) {
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
