package com.belladati.sdk.connector.example.generator;

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
 * Example implementation of {@link DataProviderApi}. It generates random values based on the configuration.
 * @author Lubomir Elko
 */
public class RandomDataProvider extends DataProviderApi<RandomRows> {
	private static final Log log = LogFactory.getLog(RandomDataProvider.class);

	/**
	 * Creates data provider that will generate random values based on the configuration.
	 * @param properties Data provider configuration
	 */
	public RandomDataProvider(Map<String, PropertyValueApi<?>> properties) {
		super(properties);
	}

	/**
	 * Returns name of this data provider that should be displayed on the user interface.
	 * @return Data provider name
	 */
	public static String getName() {
		return "BellaDati Connector SDK Example - Random Generator";
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
	public RandomRows providePreviewData(int limit) {
		log.info("Providing preview data: limit=" + limit);
		return new RandomRows(properties, true, limit);
	}

	@Override
	public RandomRows provideImportData(ProgressBarApi progressBar) {
		log.info("Providing import data: progressBar=" + progressBar);
		return new RandomRows(properties, true, progressBar);
	}

	@Override
	public RowApi provideDefaultDataDefinition() {
		log.info("Providing default column names");
		RandomRows sampleRows = new RandomRows(properties, false, 1);
		Iterator<RandomRow> iterator = sampleRows.iterator();
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
