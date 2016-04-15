package com.belladati.sdk.connector.sample;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import com.belladati.sdk.connector.DataProviderApi;
import com.belladati.sdk.connector.ReflectionsUtils;
import com.belladati.sdk.connector.sample.SampleDataProvider;

/**
 * Tests to verify behaviour of this example connector.
 * @author Lubomir Elko
 */
public class ConnectorTest {

	@Test
	public void testAvailableDataProviders() throws Throwable {
		List<String> results = ReflectionsUtils.getSubTypesOf(DataProviderApi.class);

		assertNotNull(results);
		assertEquals(results.size(), 1, "Only one implementation of connector is expected in this example");
		assertEquals(results.get(0), SampleDataProvider.class.getName());
	}

	@Test
	public void testConstructor() throws Throwable {
		Class<?>[] paramTypes = new Class<?>[] { Map.class };
		Object[] paramValues = new Object[] { null };

		Object result = ReflectionsUtils.invokeConstructor(SampleDataProvider.class.getName(), paramTypes, paramValues);
		assertNotNull(result);

		SampleDataProvider provider = (SampleDataProvider) result;
		assertNotNull(provider.getProperties());
		assertEquals(provider.getProperties().size(), 6);
	}

	@Test
	public void testGetName() throws Throwable {
		Object result = ReflectionsUtils.invokeStaticMethod(SampleDataProvider.class.getName(), "getName");
		assertNotNull(result);
		assertEquals(result.toString(), "BellaDati Connector SDK Example");
	}

	@Test
	public void testGetProperties() throws Throwable {
		SampleDataProvider provider = new SampleDataProvider(null);

		assertNotNull(provider.getDefaultProperties());
		assertEquals(provider.getDefaultProperties().size(), 6);
		assertNotNull(provider.getProperties());
		assertEquals(provider.getProperties().size(), 6);
	}

}
