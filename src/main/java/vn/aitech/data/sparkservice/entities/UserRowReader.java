/**
 * 
 */
package vn.aitech.data.sparkservice.entities;

import java.io.Serializable;
import java.sql.Timestamp;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.CassandraRowMetadata;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReader;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import scala.collection.IndexedSeq;
import vn.aitech.data.entities.User;

/**
 * @author thanhdq
 *
 */
public class UserRowReader extends GenericRowReader<User> {
	private static final long serialVersionUID = 1L;
	private static RowReader<User> reader = new UserRowReader();

	public static class UserRowReaderFactory implements RowReaderFactory<User>, Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public RowReader<User> rowReader(TableDef arg0, IndexedSeq<ColumnRef> arg1) {
			return reader;
		}

		@Override
		public Class<User> targetClass() {
			return User.class;
		}
	}

	@Override
	public User read(Row row, CassandraRowMetadata arg1) {
		User user = new User();
		user.setPhone(row.getString("phone"));
		user.setAvatarPath(row.getString("avatarpath"));
		user.setBirthDay(row.getString("birthday"));
		user.setEducation(row.getString("educaiton"));
		user.setFacebook(row.getString("fblink"));
		user.setFullName(row.getString("fullname"));
		user.setMarriedStatus(row.getString("marriedstatus"));
		user.setBirthPlaces(row.getString("birthplace"));
		user.setWorkPlaces(row.getString("workplace"));
		if (row.getTimestamp("createtime") != null) {
			user.setCreatedTime(new Timestamp(row.getTimestamp("createtime").getTime()));
		}
		return user;
	}
}
