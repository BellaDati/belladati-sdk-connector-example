package com.belladati.sdk.connector.example.sql;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.belladati.sdk.connector.RowApi;

/**
 * Example implementation of {@link RowApi}.
 * @author Lubomir Elko
 */
public class PostgreRow implements RowApi {

	/** Index of this row **/
	private final int rowIndex;

	/** Values on this row **/
	private final String[] values;

	private final PostgreRows sqlRows;

	public PostgreRow(int rowIndex, PostgreRows rows, ResultSet resultSet) throws SQLException {
		this.rowIndex = rowIndex;
		this.sqlRows = rows;
		values = new String[sqlRows.getTotalColumns()];
		for (int i = 0; i < sqlRows.getTotalColumns(); i++) {
			values[i] = resultSet.getString(i + 1);
		}
	}

	@Override
	public String[] getValues() {
		return values;
	}

	@Override
	public String getValue(int columnIndex) {
		if (columnIndex >= 0 && columnIndex < values.length) {
			return values[columnIndex];
		}
		return "";
	}

	@Override
	public int getLength() {
		return values.length;
	}

	@Override
	public int getIndex() {
		return rowIndex;
	}

}
