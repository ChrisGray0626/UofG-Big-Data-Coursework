package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

/**
 * @Description TermCount of a news structure
 * @Author Xiaohui Yu
 * @Date 2023/2/20
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class NewsTermTotal implements Serializable {

    private NewsArticle newsArticle;
    private TermCount termCount;
}
