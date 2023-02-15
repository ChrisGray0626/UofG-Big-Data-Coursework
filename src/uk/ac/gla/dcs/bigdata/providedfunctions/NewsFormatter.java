package uk.ac.gla.dcs.bigdata.providedfunctions;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

/**
 * Converts a Row containing a String Json news article into a NewsArticle object 
 * @author Richard
 *
 */
public class NewsFormatter implements FlatMapFunction<Row,NewsArticle> {

	private static final long serialVersionUID = -4631167868446468097L;

	private transient ObjectMapper jsonMapper;
	
	@Override
	public Iterator<NewsArticle> call(Row value) throws Exception {
		if (jsonMapper==null) {
			jsonMapper = new ObjectMapper();
		}
		NewsArticle article = jsonMapper.readValue(value.mkString(), NewsArticle.class);
		// Remove articles with no title
		if (null== article.getTitle()) {
			return Collections.emptyIterator();
		}

		return List.of(article).iterator();
	}
		
		
	
}
