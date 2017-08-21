import java.io.File;
//import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import com.hankcs.hanlp.corpus.dependency.CoNll.CoNLLSentence;
//import com.hankcs.hanlp.corpus.dependency.CoNll.CoNLLWord;
//import com.hankcs.hanlp.seg.Segment;
//import com.hankcs.hanlp.seg.common.Term;
//import com.hankcs.hanlp.suggest.Suggester;
//import com.hankcs.hanlp.tokenizer.NLPTokenizer;
import com.hankcs.lucene.HanLPAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
//import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.search.highlight.*;
import org.apache.lucene.search.highlight.Scorer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
//import com.hankcs.hanlp.HanLP;
//import com.hankcs.hanlp.*;

public class LuceneDemo1 {
    private static final String driverClassName="com.mysql.jdbc.Driver";
    private static final String url="jdbc:mysql://60.191.74.66:3306/most";
    private static final String username="xyq";
    private static final String password="123456";


    private static final Version version = Version.LUCENE_5_5_4;
    private Directory directory = new RAMDirectory();
    private DirectoryReader ireader = null;
    private IndexWriter iwriter = null;
    private HanLPAnalyzer analyzer;
    //存放索引文件的位置，即索引库
    private String searchDir = "d:\\Lucene\\Index";
    private static File indexFile = null;
    private Connection conn;
    /**
     * 读取索引文件
     * @return
     * @throws Exception
     */
    public IndexSearcher getSearcher() throws Exception{
        indexFile = new File(searchDir);
        ireader = DirectoryReader.open(FSDirectory.open(indexFile.toPath()));
        return new IndexSearcher(ireader);
    }
    /**
     * 获取数据库连接
     * @return
     */
    public Connection getConnection(){
        if(this.conn == null){
            try {
                Class.forName(driverClassName);
                conn = DriverManager.getConnection(url, username, password);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return conn;
    }
    /**
     * 中文分词工具
     * @return
     */
    private HanLPAnalyzer getAnalyzer(){
        if(analyzer == null){
            return new HanLPAnalyzer();
        }else{
            return analyzer;
        }
    }
    /**
     * 创建索引文件
     */
    public void createIndex(){
        Connection conn = getConnection();
        ResultSet rs = null;
        PreparedStatement pstmt = null;
        if(conn == null){
            System.out.println("get the connection error...");
            return ;
        }
        String sql = "select * from zhengfu_test";
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            /**
             * 获取索引文件位置
             */
            indexFile = new File(searchDir);
            if(!indexFile.exists()) {
                indexFile.mkdirs();
            }
            /**
             * 设置索引参数
             */
            directory = FSDirectory.open(indexFile.toPath());
            IndexWriterConfig iwConfig =  new IndexWriterConfig(getAnalyzer());
            iwConfig.setOpenMode(OpenMode.CREATE_OR_APPEND);
            iwriter = new IndexWriter(directory,iwConfig);
            /*lucene本身不支持更新
             *
             * 通过删除索引然后再建立索引来更新
             */
            iwriter.deleteAll();//删除上次的索引文件，重新生成索引
            while(rs.next()){
                int id = rs.getInt("Id");
                String name = rs.getString("content");
                String title = rs.getString("title");
                Document doc = new Document();
                doc.add(new TextField("Id", id+"",Field.Store.YES));
                doc.add(new TextField("content", name+"",Field.Store.YES));
                doc.add(new TextField("title", title+"",Field.Store.YES));
                iwriter.addDocument(doc);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            try {
                if(iwriter != null)
                    iwriter.close();
                rs.close();
                pstmt.close();
                if(!conn.isClosed()){
                    conn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 查询方法
     * @param field 字段名称
     * @param keyword 字段值
     * @param num
     * @throws Exception
     */
    public QueryResult search(String field,String keyword,int num)throws Exception {
        //IndexSearcher isearcher = getSearcher();
        Analyzer analyzer = getAnalyzer();
        //使用QueryParser查询分析器构造Query对象
        QueryParser qp = new QueryParser(field, analyzer);
        qp.setDefaultOperator(QueryParser.OR_OPERATOR);
//        try {
            Query query = qp.parse(keyword);
            return search(query, num);

//        } catch (Exception e) {
//            //throw new RuntimeException(e);
//            e.printStackTrace();
//        }

    }

    /**
     * 重载查询方法
     * @param keyword 字段值
     * @param num
     * @throws Exception
     */
    public QueryResult search(String keyword, int num) throws Exception{

//        try {
            // 1，把要搜索的文本解析为 Query
            String[] fields = { "title", "content" };
            Map<String, Float> boosts = new HashMap<String, Float>();
            //创建索引时设置相关度,值越大,相关度越高,越容易查出来.name的优先级高于内容
            boosts.put("title", 3f);
            boosts.put("content", 1.0f); //默认为1.0f
            Analyzer analyzer =  getAnalyzer();
            //构造QueryParser,设置查询的方式,以及查询的字段,分词器和相关度的设置
            QueryParser queryParser = new MultiFieldQueryParser(fields, analyzer, boosts);
            //查询关键字queryString的结果
            Query query = queryParser.parse(keyword);

            //返回查询的结果
            return search(query, num);
//        } catch (Exception e) {
//            //throw new RuntimeException(e);
//            e.printStackTrace();
//        }
    }
    /**
     * 重载查询方法
     * @param query 对象
     * @param num
     * @throws Exception
     */
    public QueryResult search(Query query, int num) throws Exception{
        IndexSearcher indexSearcher = getSearcher();
        Analyzer analyzer =  getAnalyzer();

//        try {

            TopDocs topDocs = indexSearcher.search(query, num);

            //将结果显示出来,收集到总记录数,实例化recordList,收集索引记录
            int recordCount = topDocs.totalHits;
            List<Document> recordList = new ArrayList<Document>();

            //准备高亮器,字体颜色设置为red
            Formatter formatter = new SimpleHTMLFormatter("<font color='red'>", "</font>");
            Scorer scorer = new QueryScorer(query);
            Highlighter highlighter = new Highlighter(formatter, scorer);

            //设置段划分器，指定关键字所在的内容片段的长度
            Fragmenter fragmenter = new SimpleFragmenter(150);
            highlighter.setTextFragmenter(fragmenter);

            // 3，取出当前页的数据
            //获取需要查询的最后数据的索引号
            //int endResult = Math.min(num, topDocs.totalHits);
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                //ScoreDoc scoreDoc = topDocs.scoreDocs[i];
                int docSn = scoreDoc.doc; // 文档内部编号
                Document doc = indexSearcher.doc(docSn); // 根据编号取出相应的文档
                //这里的.replaceAll("\\s*", "")是必须的，\r\n这样的空白字符会导致高亮标签错位
                String content = doc.get("content").replaceAll("\\s*", "");
                String title = doc.get("title").replaceAll("\\s*", "");

                // 高亮处理，返回高亮后的结果，如果当前属性值中没有出现关键字，会返回 null
                // 查询“内容"是否包含关键字，没有则为null，有则加上高亮效果。
                String highContent = highlighter.getBestFragment(analyzer, "content", content);
                String highTitle = highlighter.getBestFragment(analyzer, "title", title);

                if (highContent == null) {
                    //如果没有关键字,则设置不能超过50个字符
                    //String content = doc.get("content");
                    int endIndex = Math.min(150, content.length());
                    highContent = content.substring(0, endIndex);// 最多前150个字符
                }
                if (highTitle == null){
                    highTitle = title;
                }
                Document doc_ = new Document();
                doc_.add(new TextField("title", highTitle, Field.Store.YES));
                doc_.add(new TextField("content", highContent, Field.Store.YES));
                System.out.print(doc_.get("title")+" ");
                System.out.println(doc_.get("content")+" ");
                //返回高亮后的结果或者没有高亮但是不超过50个字符的结果
                //doc.getField("content").setValue(highContent);

                //recordList收集索引记录
                recordList.add(doc_);
            }

            // 返回结果QueryResult
            return new QueryResult(recordCount, recordList);
//        } catch (Exception e) {
////            throw new RuntimeException(e);
//            e.printStackTrace();
//
//        }
    }
    /**
     * 搜索索引
     */
    public static void main(String[] args) {
        try {
            LuceneDemo1 ld = new LuceneDemo1();
            ld.createIndex();
            long startTime = System.currentTimeMillis();    //获取开始时间
            QueryResult qr = null;
            qr = ld.search("农业科技创新", 10);
            long endTime = System.currentTimeMillis(); //获取结束时间
            System.out.println("程序运行时间：" + (endTime - startTime) + "ms");
        }catch(Exception e){
            e.printStackTrace();
        }

    }
}
