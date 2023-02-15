package uk.ac.gla.dcs.bigdata.studentstructures;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

/**
 * @Description Text Entity
 * @Author Xiaohui Yu
 * @Date 2023/2/9
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Text {

    private NewsArticle newsArticle;
    private String text;
}
