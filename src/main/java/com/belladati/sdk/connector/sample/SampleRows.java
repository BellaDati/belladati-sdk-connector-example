package com.belladati.sdk.connector.sample;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;

import com.belladati.sdk.connector.ProgressBarApi;
import com.belladati.sdk.connector.PropertyValueApi;
import com.belladati.sdk.connector.RowsApi;

/**
 * Example implementation of {@link Rows}.
 * @author Lubomir Elko
 */
public class SampleRows implements RowsApi<SampleRow> {

	private final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	private final Map<String, PropertyValueApi<?>> properties;
	private final boolean skipHeaders;
	private final int limit;
	private final ProgressBarApi progressBar;
	private String[] columnNames;

	public SampleRows(Map<String, PropertyValueApi<?>> properties, boolean skipHeaders, ProgressBarApi progressBar) {
		this(properties, skipHeaders, -1, progressBar);
	}

	public SampleRows(Map<String, PropertyValueApi<?>> properties, boolean skipHeaders, Integer limit) {
		this(properties, skipHeaders, limit, null);
	}

	private SampleRows(Map<String, PropertyValueApi<?>> properties, boolean skipHeaders, Integer limit,
		ProgressBarApi progressBar) {
		this.properties = properties;
		this.skipHeaders = skipHeaders;
		this.limit = limit;
		this.progressBar = progressBar;
	}

	@Override
	public Iterator<SampleRow> iterator() {
		return new SampleRowsIterator();
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public String[] getColumns() {
		return columnNames;
	}

	/**
	 * An iterator over a random generated {@link SampleRow}s.
	 * @author Lubomir Elko
	 */
	private class SampleRowsIterator implements Iterator<SampleRow> {

		private int index = 0;
		private final int totalRows, totalColumns, numberOfAttributes, numberOfIndicators;
		private final String attributePrefix;

		public SampleRowsIterator() {
			this.totalRows = getIntValue("numberOfRows");
			this.numberOfAttributes = getIntValue("numberOfAttributes");
			this.numberOfIndicators = getIntValue("numberOfIndicators");
			this.totalColumns = 1 + numberOfAttributes + numberOfIndicators;
			this.attributePrefix = properties.get("attributePrefix").getValueOrDefaultAsString();
		}

		@Override
		public boolean hasNext() {
			if (columnNames == null) {
				columnNames = getHeaders().toArray(new String[totalColumns]);
			}

			final int nextIndex = skipHeaders ? (index + 1) : index;

			if (nextIndex > totalRows) {
				// we exceeded total number of rows
				return false;
			}
			if (limit != -1 && nextIndex > limit) {
				// we exceeded required limit
				return false;
			}
			return true;
		}

		@Override
		public SampleRow next() {
			if (progressBar != null) {
				progressBar.set(getPercent(index, totalColumns));
			}

			if (!skipHeaders && index == 0) {
				return new SampleRow(index++, getHeaders().toArray(new String[totalColumns]));
			} else {
				return new SampleRow(index++, generateValues().toArray(new String[totalColumns]));
			}
		}

		private List<String> getHeaders() {
			ArrayList<String> values = new ArrayList<String>(totalColumns);
			values.add("Date and time");
			for (int index = 0; index < numberOfAttributes; index++) {
				values.add("Attribute " + (index + 1));
			}
			for (int index = 0; index < numberOfIndicators; index++) {
				values.add("Indicator " + (index + 1));
			}
			return values;
		}

		private List<String> generateValues() {
			ArrayList<String> values = new ArrayList<String>(totalColumns);
			values.add(DATE_FORMAT.format(new Date()));
			for (int index = 0; index < numberOfAttributes; index++) {
				values.add(attributePrefix + (index + 1) + " " + getRandomLetter());
			}
			for (int index = 0; index < numberOfIndicators; index++) {
				values.add((index + 1) + getRandomNumber());
			}
			return values;
		}

		private String getRandomNumber() {
			return RandomStringUtils.randomNumeric(4);
		}

		private String getRandomLetter() {
			return RandomStringUtils.randomAlphabetic(1).toUpperCase();
		}

		private int getIntValue(String key) {
			return Integer.valueOf(properties.get(key).getValueOrDefaultAsString());
		}

		private int getPercent(long value, long max) {
			if (max == 0) {
				return 0;
			}
			return (int) (((float) value / max) * 100);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

}
