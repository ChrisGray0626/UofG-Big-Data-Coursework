package uk.ac.gla.dcs.bigdata.studentstructures;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description Text Entity
 * @Author Chris
 * @Date 2023/2/9
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Text {

    private String newsArticleId;
    private String text;
}
