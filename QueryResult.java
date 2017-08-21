import java.util.List;

import org.apache.lucene.document.Document;

/**
 * 查询结果类,就像实体
 * @author liu
 *
 */
public class QueryResult {
    //总记录数
    private int recordCount;
    //索引记录
    private List<Document> recordList;

    //有参构造方法
    public QueryResult(int recordCount, List<Document> recordList) {
        super();
        this.recordCount = recordCount;
        this.recordList = recordList;
    }

    //get,set方法
    public int getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(int recordCount) {
        this.recordCount = recordCount;
    }

    public List<Document> getRecordList() {
        return recordList;
    }

    public void setRecordList(List<Document> recordList) {
        this.recordList = recordList;
    }
}