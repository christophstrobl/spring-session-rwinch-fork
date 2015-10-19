/*
 * Copyright 2002-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.session.data.redis;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.RedisKeyValueAdapter;
import org.springframework.data.redis.core.RedisKeyValueTemplate;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.convert.IndexResolverImpl;
import org.springframework.data.redis.core.convert.MappingRedisConverter;
import org.springframework.data.redis.core.convert.ReferenceResolver;
import org.springframework.data.redis.core.convert.ReferenceResolverImpl;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.session.data.redis.config.annotation.web.http.EnableRedisHttpSession;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
@WebAppConfiguration
public class RedisKeyValueAdapterTests {

	RedisKeyValueTemplate template;

	@Autowired 
	RedisOperations<Object, Object> redis;

	@Before
	public void setup() {

		RedisMappingContext mappingContext = new RedisMappingContext();
		MappingRedisConverter converter = new MappingRedisConverter(mappingContext, new IndexResolverImpl(), null);
		RedisKeyValueAdapter adapter = new RedisKeyValueAdapter(redis, converter);
		ReferenceResolver resolver = new ReferenceResolverImpl(adapter);
		converter.setReferenceResolver(resolver);
		converter.afterPropertiesSet();
		mappingContext.afterPropertiesSet();

		template = new RedisKeyValueTemplate(adapter, mappingContext);

		template.delete(200, RedisSession.class);
	}

	@Test
	public void saves() throws InterruptedException {

		RedisSession session = new RedisSession();
		session.setId(200);
		session.setAttribute("hi", "there");

		template.insert(session);
	}

	@RedisHash("sessions")
	public static class RedisSession {

		@Id Integer id;

		String other = "other";

		Map<String, Object> attribute = new HashMap<String, Object>();

		public void setAttribute(String attrName, Object attrValue) {
			attribute.put(attrName, attrValue);
		}

		public void setId(int id) {
			this.id = id;
		}

		public int getId() {
			return id;
		}

	}

	@Configuration
	@EnableRedisHttpSession(redisNamespace = "RedisOperationsSessionRepositoryITests")
	static class Config {
		@Bean
		public JedisConnectionFactory connectionFactory() throws Exception {
			JedisConnectionFactory factory = new JedisConnectionFactory();
			factory.setUsePool(false);
			return factory;
		}
	}
}
