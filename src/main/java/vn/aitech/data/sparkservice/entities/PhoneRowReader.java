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

/**
 * @author thanhdq
 *
 */
public class PhoneRowReader extends GenericRowReader<String> {

	private static final long serialVersionUID = 1L;
	private static RowReader<String> reader = new PhoneRowReader();
	public static class PhoneRowReaderFactory implements RowReaderFactory<String>, Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public RowReader<String> rowReader(TableDef arg0, IndexedSeq<ColumnRef> arg1) {
			return reader;
		}

		@Override
		public Class<String> targetClass() {
			return String.class;
		}

	}

	@Override
	public String read(Row phoneRow, CassandraRowMetadata arg1) {
		return phoneRow.getString("phone");
	}

}
