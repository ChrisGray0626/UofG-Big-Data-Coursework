package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.accumulator.TermNumMapAccumulator;
import uk.ac.gla.dcs.bigdata.constant.ValueConstant;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormatter;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormatter;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.DocDPHScorer;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsToTermCountInDoc;
import uk.ac.gla.dcs.bigdata.studentfunctions.QueryDocScoreToDocumentRanking;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryDocScore;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryTerm;
import uk.ac.gla.dcs.bigdata.studentstructures.TermCount;

/**
 * This is the main class where your Spark topology should be specified.
 * <p>
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 *
 * @author Richard
 */
@Slf4j
public class AssessedExercise {

    public static void main(String[] args) {

        File hadoopDIR = new File(
                "resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
        System.setProperty("hadoop.home.dir",
                hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it

        // The code submitted for the assessed exerise may be run in either local or remote modes
        // Configuration of this will be performed based on an environment variable
        String sparkSessionName = "BigDataAE"; // give the session a name

        // Create the Spark Configuration
        SparkConf conf = new SparkConf()
                .setMaster(ValueConstant.SPARK_MASTER)
                .set("spark.executor.memory", ValueConstant.EXECUTOR_MEMORY)
                .set("spark.executor.cores", ValueConstant.EXECUTOR_CORE_NUM)
                .setAppName(sparkSessionName);
        // Create the spark session
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();


        // Get the location of the input queries
        String queryFile = System.getenv("bigdata.queries");
        if (queryFile == null) {
            queryFile = "data/queries.list"; // default is a sample with 3 queries
        }

        // Get the location of the input news articles
        String newsFile = System.getenv("bigdata.news");
        if (newsFile == null) {
            newsFile = ValueConstant.NEWS_FILE_PATH; // default is a sample of 5000 news articles
        }

        // Call the student's code
        List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

        // Close the spark session
        spark.close();

        // Check if the code returned any results
        if (results == null) {
            System.err.println(
                    "Topology return no rankings, student code may not be implemented, skiping final write.");
        } else {

            // We have set of output rankings, lets write to disk

            // Create a new folder
            File outDirectory = new File("results/" + System.currentTimeMillis());
            if (!outDirectory.exists()) {
                outDirectory.mkdir();
            }

            // Write the ranking for each query as a new file
            for (DocumentRanking rankingForQuery : results) {
                rankingForQuery.write(outDirectory.getAbsolutePath());
            }
        }


    }

    public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {

        // Load queries and news articles
        Dataset<Row> queryJson = spark.read().text(queryFile);
        Dataset<Row> newsJson = spark.read().text(newsFile); // read in files as string rows, one row per article
        // Limit the number of news
        // newsJson = newsJson.limit(40000);
        // Repartition the news
        // newsJson = newsJson.repartition(Integer.parseInt(ValueConstant.PARTITION_NUM));
        // Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
        Dataset<Query> queries = queryJson.map(new QueryFormatter(),
                Encoders.bean(Query.class)); // this converts each row into a Query
        // Convert queries to query terms
        Dataset<QueryTerm> queryTerms = queries.flatMap((FlatMapFunction<Query, QueryTerm>) query -> {
            List<QueryTerm> terms = new ArrayList<>();
            query.getQueryTerms().forEach(queryTerm -> terms.add(new QueryTerm(query, queryTerm)));
            return terms.iterator();
        }, Encoders.bean(QueryTerm.class));
        // Convert query terms to query term map
        Map<String, Query> queryTermMap =
                queryTerms.collectAsList().stream().collect(Collectors.toMap(QueryTerm::getTerm, QueryTerm::getQuery));
        Broadcast<Map<String, Query>> queryTermMapBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryTermMap);
        // Accumulators for term count in corpus
        LongAccumulator docTotalInCorpusAccumulator = spark.sparkContext().longAccumulator();
        TermNumMapAccumulator termNumMapInCorpusAccumulator = new TermNumMapAccumulator();
        LongAccumulator termTotalInCorpusAccumulator = spark.sparkContext().longAccumulator();
        // Register custom accumulator
        spark.sparkContext().register(termNumMapInCorpusAccumulator, "termNumMapInCorpusAccumulator");
        // Convert news json to news
        Dataset<NewsArticle> news = newsJson.flatMap(new NewsFormatter(),
                Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
        // Calculate term count in document
        Dataset<TermCount> termCountInDoc = news.flatMap(new NewsToTermCountInDoc(queryTermMapBroadcast, termNumMapInCorpusAccumulator, docTotalInCorpusAccumulator, termTotalInCorpusAccumulator), Encoders.bean(TermCount.class));
        // Active the above transformation due to accumulator feature
        log.info("termCountInDoc count: " + termCountInDoc.cache().count());
        // Broadcast term count in corpus from accumulator
        long docTotalInCorpus = docTotalInCorpusAccumulator.value();
        Broadcast<Long> docTotalInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(docTotalInCorpus);
        Map<String, Long> termNumMapInCorpus = termNumMapInCorpusAccumulator.value();
        Broadcast<Map<String, Long>> termNumMapInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(termNumMapInCorpus);
        long termTotalInCorpus = termTotalInCorpusAccumulator.value();
        Broadcast<Long> termTotalInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(termTotalInCorpus);
        // Calculate score for each query term in each document
        Dataset<QueryDocScore> queryDocScores = termCountInDoc.flatMap(
                new DocDPHScorer(docTotalInCorpusBroadcast, termNumMapInCorpusBroadcast, termTotalInCorpusBroadcast), Encoders.bean(
                        QueryDocScore.class));
        // Convert to result
        List<DocumentRanking> documentRankings = queryDocScores.groupByKey(
                        (MapFunction<QueryDocScore, String>) (score) -> score.getQuery().getOriginalQuery(),
                        Encoders.STRING()).mapGroups(new QueryDocScoreToDocumentRanking(), Encoders.bean(DocumentRanking.class))
                .collectAsList();

        return documentRankings;
    }
}
