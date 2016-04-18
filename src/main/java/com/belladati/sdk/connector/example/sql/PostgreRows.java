package com.belladati.sdk.connector.example.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.belladati.sdk.connector.ConnectorUtils;
import com.belladati.sdk.connector.ProgressBarApi;
import com.belladati.sdk.connector.RowsApi;

/**
 * Example implementation of {@link RowsApi}.
 * @author Lubomir Elko
 */
public class PostgreRows implements RowsApi<PostgreRow> {
	private final static Log log = LogFactory.getLog(PostgreRows.class);

	/** Precompiled SQL statement for data **/
	private final PreparedStatement dataStatement;

	/** Precompiled SQL statement for number of records **/
	private PreparedStatement sizeStatement;

	/** Reference to progress bar displayed on user interface during import **/
	private final ProgressBarApi progressBar;

	/** Total number of rows **/
	private int totalRows;

	/** Total number of columns **/
	private int totalColumns;

	/** Metadata for columns in {@link ResultSet} **/
	private ResultSetMetaData metaData;

	/**
	 * Creates object responsible for providing source rows based on the given SQL statement.
	 * @param dataStatement Precompiled SQL statement for number of records
	 */
	public PostgreRows(PreparedStatement dataStatement) {
		this(dataStatement, null, null);
	}

	/**
	 * Creates object responsible for providing source rows based on the given SQL statements.
	 * @param dataStatement Precompiled SQL statement for number of records
	 * @param sizeStatement Precompiled SQL statement for number of records
	 * @param progressBar Reference to progress bar displayed on user interface during import
	 */
	public PostgreRows(PreparedStatement dataStatement, PreparedStatement sizeStatement, ProgressBarApi progressBar) {
		this.dataStatement = dataStatement;
		this.sizeStatement = sizeStatement;
		this.progressBar = progressBar;
	}

	@Override
	public Iterator<PostgreRow> iterator() {
		try {
			// load total number of rows only once
			if (sizeStatement != null) {
				ResultSet resultSet = sizeStatement.executeQuery();
				try {
					resultSet.next();
					totalRows = resultSet.getInt(1);
				} catch (Exception e) {
					log.warn("Cannot get total number of rows", e);
				} finally {
					if (resultSet != null) {
						resultSet.close();
					}
					sizeStatement = null;
				}
			}

			// load result set
			ResultSet rs = dataStatement.executeQuery();
			metaData = rs.getMetaData();
			totalColumns = metaData.getColumnCount();
			return new PostgreRowsIterator(rs);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * An iterator over a database result set.
	 * @author Lubomir Elko
	 */
	private class PostgreRowsIterator implements Iterator<PostgreRow> {

		/** Current index/position **/
		private int rowIndex = 1;

		/** Database result set **/
		private final ResultSet resultSet;

		/** Flag indicating if the iteration has more elements **/
		private boolean hasNext;

		/**
		 * Creates {@link Iterator} that will iterate over given database result set.
		 * @param resultSet Database result set
		 */
		public PostgreRowsIterator(ResultSet resultSet) {
			this.resultSet = resultSet;
		}

		@Override
		public boolean hasNext() {
			if (hasNext) {
				return true;
			}
			try {
				return (hasNext = resultSet.next());
			} catch (SQLException e) {
				throw new IllegalStateException(e);
			}
		}

		@Override
		public PostgreRow next() {
			if (!hasNext) {
				throw new IllegalStateException("No more entries!");
			}
			try {
				ConnectorUtils.updateProgressBar(progressBar, rowIndex, totalRows);
				hasNext = false;
				return new PostgreRow(rowIndex++, PostgreRows.this, resultSet);
			} catch (SQLException e) {
				throw new IllegalStateException(e);
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

	/**
	 * Returns total number of columns
	 * @return Total number of columns
	 */
	protected int getTotalColumns() {
		return totalColumns;
	}

	@Override
	public String[] getColumns() {
		if (metaData != null) {
			List<String> l = new ArrayList<String>();
			for (int i = 1; i < getTotalColumns(); i++) {
				try {
					l.add(i, metaData.getColumnName(i));
				} catch (Exception e) {
					log.warn("Cannot get column names", e);
					return null;
				}
			}
			return l.toArray(new String[l.size()]);
		}
		return null;
	}

	@Override
	public void close() {
		try {
			if (dataStatement != null) {
				Connection conn = dataStatement.getConnection();
				if (conn != null && !conn.isClosed()) {
					conn.commit();
					conn.close();
				}
			}
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}

}
