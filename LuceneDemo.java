
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.hankcs.lucene.HanLPAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;


public class LuceneDemo {
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
                Document doc = new Document();
                doc.add(new TextField("Id", id+"",Field.Store.YES));
                doc.add(new TextField("content", name+"",Field.Store.YES));
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
    public void searchByTerm(String field,String keyword,int num) throws Exception{
        IndexSearcher isearcher = getSearcher();
        Analyzer analyzer =  getAnalyzer();
        //使用QueryParser查询分析器构造Query对象
        QueryParser qp = new QueryParser(field,analyzer);
        qp.setDefaultOperator(QueryParser.OR_OPERATOR);
        try {
            Query query = qp.parse(keyword);
            ScoreDoc[] hits;


            //注意searcher的几个方法
            hits = isearcher.search(query, num).scoreDocs;


            System.out.println("the ids is =");
            for (int i = 0; i < hits.length; i++) {
                Document doc = isearcher.doc(hits[i].doc);
                System.out.print(doc.get("Id")+" ");
                System.out.println(doc.get("content")+" ");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
    /**
     * 搜索索引
     */
    public static void main(String[] args) throws Exception {
        LuceneDemo ld = new LuceneDemo();
        ld.createIndex();
        long startTime = System.currentTimeMillis();    //获取开始时间
        ld.searchByTerm("content", "农业科技创新", 10);
        long endTime=System.currentTimeMillis(); //获取结束时间
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms");

    }
}
