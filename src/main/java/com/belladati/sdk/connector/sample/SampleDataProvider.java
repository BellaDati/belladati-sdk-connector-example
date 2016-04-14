package com.belladati.sdk.connector.sample;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.belladati.sdk.connector.DataProviderApi;
import com.belladati.sdk.connector.ProgressBarApi;
import com.belladati.sdk.connector.PropertyValueApi;
import com.belladati.sdk.connector.PropertyValueApi.BooleanValue;
import com.belladati.sdk.connector.PropertyValueApi.IntegerValue;
import com.belladati.sdk.connector.PropertyValueApi.StringValue;
import com.belladati.sdk.connector.RowApi;

/**
 * Example implementation of {@link DataProviderApi}.
 * @author Lubomir Elko
 */
public class SampleDataProvider extends DataProviderApi<SampleRows> {
	private static final Log log = LogFactory.getLog(SampleDataProvider.class);

	public SampleDataProvider(Map<String, PropertyValueApi<?>> properties) {
		super(properties);
	}

	/**
	 * Returns name of this data provider that should be displayed on the user interface.
	 * @return Data provider name
	 */
	public static String getName() {
		return "BellaDati Connector SDK Example";
	}

	@Override
	public Map<String, PropertyValueApi<?>> getDefaultProperties() {
		Map<String, PropertyValueApi<?>> defaults = new HashMap<String, PropertyValueApi<?>>();
		defaults.put("numberOfIndicators", new IntegerValue(3, true));
		defaults.put("numberOfAttributes", new IntegerValue(4, true));
		defaults.put("numberOfRows", new IntegerValue(5, true));
		defaults.put("attributePrefix", new StringValue("Sample", true));
		defaults.put("failsOnValidation", new BooleanValue(false, true));
		defaults.put("optionalStringField", new StringValue(null, false));
		return defaults;
	}

	@Override
	public SampleRows provideImportData(ProgressBarApi progressBar) {
		log.info("Providing import data: progressBar=" + progressBar);
		return new SampleRows(properties, true, progressBar);
	}

	@Override
	public SampleRows providePreviewData(int limit) {
		log.info("Providing preview data: limit=" + limit);
		return new SampleRows(properties, true, limit);
	}

	@Override
	public RowApi provideDefaultDataDefinition() {
		log.info("Providing default column names");
		SampleRows sampleRows = new SampleRows(properties, false, 1);
		Iterator<SampleRow> iterator = sampleRows.iterator();
		try {
			if (iterator.hasNext()) {
				return iterator.next();
			}
		} finally {
			try {
				sampleRows.close();
			} catch (Exception e) {};
		}
		return null;
	}

	@Override
	public boolean check() throws Throwable {
		log.info("Checking availability");
		return true;
	}

	@Override
	public List<String> validate() {
		List<String> errors = new ArrayList<String>();
		for (Entry<String, PropertyValueApi<?>> entry : properties.entrySet()) {
			if (!entry.getValue().isValid()) {
				errors.add("You must provide a value for " + entry.getKey());
			}
			if (entry.getKey().equalsIgnoreCase("failsOnValidation")) {
				Boolean shouldFail = Boolean.valueOf(entry.getValue().getValueOrDefaultAsString());
				if (shouldFail.booleanValue()) {
					errors.add("Validation failed due to your choice in property 'failsOnValidation'");
				}
			}
		}
		log.info("Configuration validation ended with " + errors.size() + " errors.");
		return errors;
	}

}
