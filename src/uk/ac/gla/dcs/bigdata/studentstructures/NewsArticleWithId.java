package uk.ac.gla.dcs.bigdata.studentstructures;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

/**
 * @Description
 * @Author Xiaohui Yu
 * @Date 2023/2/19
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class NewsArticleWithId {

    private String newsArticleId;
    private NewsArticle newsArticle;
}
