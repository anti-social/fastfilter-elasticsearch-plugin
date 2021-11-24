/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.elasticsearch.lsena.fastfilter;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.FilterScript.LeafFactory;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.index.fielddata.ScriptDocValues;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import java.util.Base64;
import java.nio.ByteBuffer;

import org.roaringbitmap.RoaringBitmap;

/**
 * An example script plugin that adds a {@link ScriptEngine}
 * implementing fast_filter.
 */
public class FastFilterPlugin extends Plugin implements ScriptPlugin {

	@Override
	public ScriptEngine getScriptEngine(
			Settings settings,
			Collection<ScriptContext<?>> contexts
			) {
		return new MyFastFilterEngine();
	}

	// tag::fast_filter
	private static class MyFastFilterEngine implements ScriptEngine {
		@Override
		public String getType() {
			return "fast_filter";
		}

		@Override
		public <T> T compile(
				String scriptName,
				String scriptSource,
				ScriptContext<T> context,
				Map<String, String> params
				) {
			if (!context.equals(FilterScript.CONTEXT)) {
				throw new IllegalArgumentException(getType()
						+ " scripts cannot be used for context ["
						+ context.name + "]");
			}
			// we use the script "source" as the script identifier
			// in this case, we use the name fast_filter
			if ("fast_filter".equals(scriptSource)) {
				FilterScript.Factory factory = new FastFilterFactory();
				return context.factoryClazz.cast(factory);
			}
			throw new IllegalArgumentException("Unknown script name "
					+ scriptSource);
		}

		@Override
		public void close() {
			// optionally close resources
		}

		@Override
		public Set<ScriptContext<?>> getSupportedContexts() {
			return this.getSupportedContexts();
			//return Set.of(ScoreScript.CONTEXT);
		}

		private static class FastFilterFactory implements FilterScript.Factory,
		ScriptFactory {
			@Override
			public boolean isResultDeterministic() {
				// FastFilterLeafFactory only uses deterministic APIs, this
				// implies the results are cacheable.
				return true;
			}

			@Override
			public LeafFactory newFactory(
					Map<String, Object> params,
					SearchLookup lookup
					) {
				if (!params.containsKey("field")) {
					throw new IllegalArgumentException(
						"Missing parameter [field]");
				}
				if (!params.containsKey("terms")) {
					throw new IllegalArgumentException(
						"Missing parameter [terms]");
				}

				final byte[] decodedTerms = Base64.getDecoder().decode(params.get("terms").toString());
				final ByteBuffer buffer = ByteBuffer.wrap(decodedTerms);
				RoaringBitmap rBitmap = new RoaringBitmap();
				try {
					rBitmap.deserialize(buffer);
				}
				catch (IOException e) {
					throw ExceptionsHelper.convertToElastic(e);
				}

				Object operation = params.get("operation");
				String opType = "include";
				if (operation != null) {
					opType = operation.toString();
				}
				switch (opType) {
					case "include":
						return new FastFilterLeafFactory(params, lookup, rBitmap, true);
					case "exclude":
						return new FastFilterLeafFactory(params, lookup, rBitmap, false);
					default:
						throw new IllegalStateException("Unreacable");
				}
			}
		}

		private static class FastFilterLeafFactory implements LeafFactory {
			private final Map<String, Object> params;
			private final SearchLookup lookup;
			private final IndexFieldData<?> fieldData;
			private final RoaringBitmap rBitmap;
			private final boolean found;
			private final boolean notFound;

			private FastFilterLeafFactory(
				Map<String, Object> params, SearchLookup lookup, RoaringBitmap rBitmap, boolean include
			) {
				this.params = params;
				this.lookup = lookup;
				this.rBitmap = rBitmap;
				this.found = include;
				this.notFound = !found;

				String fieldName = params.get("field").toString();
				MappedFieldType fieldType = lookup.fieldType(fieldName);
				if (!fieldType.hasDocValues()) {
					throw new ElasticsearchException("Required doc values for field: " + fieldName);
				}

				if (!(fieldType instanceof NumberFieldMapper.NumberFieldType)) {
					throw new ElasticsearchException("Field " + fieldName + " must be of integer numeric type");
				}
				IndexNumericFieldData.NumericType numericType =
					((NumberFieldMapper.NumberFieldType) fieldType).numericType();
				if (numericType.isFloatingPoint()) {
					throw new ElasticsearchException("Field " + fieldName + " must be of integer numeric type");
				}
				fieldData = lookup.getForField(fieldType);
			}

			@Override
			public FilterScript newInstance(LeafReaderContext context) {
				ScriptDocValues.Longs scriptValues = (ScriptDocValues.Longs) fieldData.load(context).getScriptValues();

				return new FilterScript(params, lookup, context) {
					@Override
					public void setDocument(int docid) {
						super.setDocument(docid);
						try {
							scriptValues.setNextDocId(docid);
						} catch (IOException e) {
							throw ExceptionsHelper.convertToElastic(e);
						}
					}

					@Override
					public boolean execute() {
						if (scriptValues.size() == 0) {
							return notFound;
						}
						for (Long scriptValue : scriptValues) {
							int value = Math.toIntExact(scriptValue);
							if (rBitmap.contains(value)) {
								return found;
							}
						}
						return notFound;
					}
				};
			}
		}
	}
	// end::fast_filter
}
