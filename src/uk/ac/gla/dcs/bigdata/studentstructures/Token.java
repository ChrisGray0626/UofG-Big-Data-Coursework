package uk.ac.gla.dcs.bigdata.studentstructures;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description Token Entity
 * @Author Chris
 * @Date 2023/2/8
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Token {

    private String newsArticleId;
    private String token;

}
