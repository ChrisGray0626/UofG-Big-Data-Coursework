package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.function.FlatMapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.Text;

/**
 * @Description Convert a NewsArticle to a list of Text
 * @Author Chris
 * @Date 2023/2/9
 */
public class NewsArticleToText implements FlatMapFunction<NewsArticle, Text> {

    @Override
    public Iterator<Text> call(NewsArticle newsArticle) throws Exception {
        List<Text> texts = new ArrayList<>();
        // Add the title
        String newsArticleId = newsArticle.getId();
        texts.add(new Text(newsArticleId, newsArticle.getTitle()));
        // Add the first 5 paragraph
        List<String> contents =
                newsArticle.getContents().stream().filter(content -> "paragraph".equals(content.getSubtype())).limit(5)
                        .map(ContentItem::getContent).collect(Collectors.toList());
        for (String content : contents) {
            texts.add(new Text(newsArticleId, content));
        }

        return texts.iterator();
    }
}
