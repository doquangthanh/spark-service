/**
 * 
 */
package vn.aitech.data.sparkservice.entities;

import java.io.Serializable;

import com.datastax.driver.core.Row;
import com.datastax.spark.connector.CassandraRowMetadata;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReader;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import scala.collection.IndexedSeq;
import vn.aitech.data.entities.Pageview;

/**
 * @author thanhdq
 *
 */
public class PageviewRowReader extends GenericRowReader<Pageview> {
	private static final long serialVersionUID = 1L;
    private static RowReader<Pageview> reader = new PageviewRowReader();

    public static class PageviewRowReaderFactory implements RowReaderFactory<Pageview>, Serializable{
        private static final long serialVersionUID = 1L;

        @Override
        public RowReader<Pageview> rowReader(TableDef arg0, IndexedSeq<ColumnRef> arg1) {
            return reader;
        }

        @Override
        public Class<Pageview> targetClass() {
            return Pageview.class;
        }
    }

	@Override
	public Pageview read(Row row, CassandraRowMetadata arg1) {
		Pageview pageview=new Pageview();
		pageview.setBrowser(row.getString("browser"));
		pageview.setTelephone(row.getString("phone"));
		pageview.setTelco(row.getString("telco"));	
		pageview.setBrowser_type(row.getString("browser_type"));
		pageview.setCity(row.getString("city"));
		pageview.setDm(row.getString("domain"));
		pageview.setHour(row.getInt("hour"));
		pageview.setId(row.getString("clientid"));
		pageview.setIds(row.getString("siteid"));
		pageview.setIdu(row.getUUID("idu"));
		pageview.setIp(row.getString("ip"));
		pageview.setLocation(row.getString("location"));
		pageview.setNv(row.getLong("timevisit"));
		pageview.setOs(row.getString("os"));
		pageview.setOs_version(row.getString("os_version"));
		pageview.setReferrer(row.getString("referer"));
		pageview.setRes(row.getString("resolution"));
		pageview.setTb(row.getLong("timesetid"));
		pageview.setTc(row.getLong("timerequest"));
		pageview.setTgl(row.getLong("timegetlocation"));
		pageview.setTitle(row.getString("title"));
		pageview.setTp(row.getLong("timeload"));
		pageview.setDetectedmobile(row.getBool("detectedmobile"));
		pageview.setDetectedinternet(row.getBool("detectedinternet"));
		pageview.setUrl(row.getString("url"));
		pageview.setUser_agent(row.getString("user_agent"));		
		return pageview;
	}

}
