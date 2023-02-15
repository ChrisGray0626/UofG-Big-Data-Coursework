package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.security.cert.CollectionCertStoreParameters;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.Text;

/**
 * @Description Convert a NewsArticle to texts
 * @Author Xiaohui Yu
 * @Date 2023/2/9
 */
public class NewsToText implements FlatMapFunction<NewsArticle, Text> {

    private final LongAccumulator docTotalInCorpusAccumulator;

    public NewsToText(LongAccumulator docTotalInCorpusAccumulator) {
        this.docTotalInCorpusAccumulator = docTotalInCorpusAccumulator;
    }

    @Override
    public Iterator<Text> call(NewsArticle newsArticle) throws Exception {
        String title = newsArticle.getTitle();
        // Remove the null title news
        if (title == null || title.isEmpty()) {
            return Collections.emptyIterator();
        }
        List<Text> texts = new ArrayList<>();
        // Add the title
        texts.add(new Text(newsArticle, title));
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
        for (String content : contents) {
            texts.add(new Text(newsArticle, content));
        }
        docTotalInCorpusAccumulator.add(1);

        return texts.iterator();
    }
}
