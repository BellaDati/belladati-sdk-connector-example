package com.belladati.sdk.connector.sample;

import com.belladati.sdk.connector.RowApi;

/**
 * Example implementation of {@link RowApi}.
 * @author Lubomir Elko
 */
public class SampleRow implements RowApi {

	private final int rowIndex;
	private final String[] values;

	public SampleRow(int rowIndex, String[] values) {
		this.rowIndex = rowIndex;
		this.values = values;
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
