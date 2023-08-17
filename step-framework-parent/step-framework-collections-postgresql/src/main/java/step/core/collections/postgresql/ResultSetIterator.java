package step.core.collections.postgresql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class ResultSetIterator implements Iterator<String> {

	private ResultSet resultSet;
	boolean hasNext;

	public ResultSetIterator(ResultSet resultSet) {
		this.resultSet = resultSet;
		try {
			this.hasNext = resultSet.next();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean hasNext() {
		return hasNext;
	}

	@Override
	public String next() {
		try {
			String string = resultSet.getString(2);
			hasNext = resultSet.next();
			return string;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}