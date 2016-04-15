package com.belladati.sdk.connector.sample;

import com.belladati.sdk.connector.RowApi;

/**
 * Example implementation of {@link RowApi}.
 * @author Lubomir Elko
 */
public class SampleRow implements RowApi {

	/** Index of this row **/
	private final int rowIndex;

	/** Values on this row **/
	private final String[] values;

	/**
	 * Creates row with given {@code rowIndex} and {@code values}.
	 * @param rowIndex Index of this row
	 * @param values Values on this row
	 */
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
