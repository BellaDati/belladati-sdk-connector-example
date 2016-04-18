package com.belladati.sdk.connector.example.sql;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import com.belladati.sdk.connector.ConnectorUtils;
import com.belladati.sdk.connector.DataProviderApi;
import com.belladati.sdk.connector.example.generator.RandomDataProvider;
import com.belladati.sdk.connector.example.sql.PostgreDataProvider;

/**
 * Tests to verify behaviour of this example connector.
 * @author Lubomir Elko
 */
public class PostgreConnectorTest {

	@Test
	public void testAvailableDataProviders() throws Throwable {
		List<String> results = ConnectorUtils.getSubTypesOf(DataProviderApi.class);

		assertNotNull(results);
		assertEquals(results.size(), 2, "Two implementations of connector are expected in this example");

		assertTrue(results.contains(RandomDataProvider.class.getName()), RandomDataProvider.class.getName());
		assertTrue(results.contains(PostgreDataProvider.class.getName()), PostgreDataProvider.class.getName());
	}

	@Test
	public void testConstructor() throws Throwable {
		Class<?>[] paramTypes = new Class<?>[] { Map.class };
		Object[] paramValues = new Object[] { null };

		Object result = ConnectorUtils.invokeConstructor(PostgreDataProvider.class.getName(), paramTypes, paramValues);
		assertNotNull(result);

		PostgreDataProvider provider = (PostgreDataProvider) result;
		assertNotNull(provider.getProperties());
		assertEquals(provider.getProperties().size(), 6);
	}

	@Test
	public void testGetName() throws Throwable {
		Object result = ConnectorUtils.invokeStaticMethod(PostgreDataProvider.class.getName(), "getName");
		assertNotNull(result);
		assertEquals(result.toString(), "BellaDati Connector SDK Example - PostgreSQL");
	}

	@Test
	public void testGetProperties() throws Throwable {
		PostgreDataProvider provider = new PostgreDataProvider(null);

		assertNotNull(provider.getDefaultProperties());
		assertEquals(provider.getDefaultProperties().size(), 6);
		assertNotNull(provider.getProperties());
		assertEquals(provider.getProperties().size(), 6);
	}

}
