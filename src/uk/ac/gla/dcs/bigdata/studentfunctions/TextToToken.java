package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.function.FlatMapFunction;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.Text;
import uk.ac.gla.dcs.bigdata.studentstructures.Token;

/**
 * @Description Convert a Text to a list of Tokens
 * @Author Chris
 * @Date 2023/2/9
 */
public class TextToToken implements FlatMapFunction<Text, Token> {

    private transient TextPreProcessor processor;

    @Override
    public Iterator<Token> call(Text text) throws Exception {
        if (processor == null) {
            processor = new TextPreProcessor();
        }

        List<Token> tokens = new ArrayList<>();
        for (String token: processor.process(text.getText())){
            tokens.add(new Token(text.getNewsArticleId(), token));
        }

        return tokens.iterator();
    }
}
