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

import static org.fest.assertions.Assertions.assertThat;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.serializer.support.DeserializingConverter;
import org.springframework.core.serializer.support.SerializingConverter;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.RedisKeyValueAdapter;
import org.springframework.data.redis.core.RedisKeyValueTemplate;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.convert.CustomConversions;
import org.springframework.data.redis.core.convert.IndexResolverImpl;
import org.springframework.data.redis.core.convert.MappingRedisConverter;
import org.springframework.data.redis.core.convert.ReferenceResolver;
import org.springframework.data.redis.core.convert.ReferenceResolverImpl;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.session.ExpiringSession;
import org.springframework.session.MapSession;
import org.springframework.session.SessionRepository;
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

	@Autowired
	SessionRepository sessions;

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

	}

	@Test
	public void saveAsLegacyAndGetAsNew() throws InterruptedException {
		SecurityContext context = SecurityContextHolder.createEmptyContext();
		User user = new User("user", "password", AuthorityUtils.createAuthorityList("ROLE_USER"));
		context.setAuthentication(new UsernamePasswordAuthenticationToken(user,user.getPassword(), user.getAuthorities()));

		ExpiringSession sessionToSave = (ExpiringSession) sessions.createSession();

		sessionToSave.setAttribute("hi", "there");
		sessionToSave.setAttribute("SECURITY_CONTEXT", context);


		sessions.save(sessionToSave);

		RedisSession session = template.findById(sessionToSave.getId(), RedisSession.class);

		assertEquals(sessionToSave, session);
	}

	@Test
	public void insertAndGetNew() {
		SecurityContext context = SecurityContextHolder.createEmptyContext();
		User user = new User("user", "password", AuthorityUtils.createAuthorityList("ROLE_USER"));
		context.setAuthentication(new UsernamePasswordAuthenticationToken(user,user.getPassword(), user.getAuthorities()));

		ExpiringSession sessionToSave = new RedisSession();

		sessionToSave.setAttribute("hi", "there");
		sessionToSave.setAttribute("SECURITY_CONTEXT", context);

		template.insert(sessionToSave);

		RedisSession actual = template.findById(sessionToSave.getId(), RedisSession.class);

		assertEquals(sessionToSave, actual);
	}

	private void assertEquals(ExpiringSession expected, ExpiringSession actual) {
		assertThat(actual).isNotNull();

		assertThat(expected.getId()).isEqualTo(actual.getId());
		assertThat(expected.getAttributeNames()).isEqualTo(actual.getAttributeNames());
		assertThat(expected.getCreationTime()).isEqualTo(expected.getCreationTime());
		assertThat(expected.getLastAccessedTime()).isEqualTo(actual.getLastAccessedTime());
		assertThat(expected.getMaxInactiveIntervalInSeconds()).isEqualTo(actual.getMaxInactiveIntervalInSeconds());
		for(String attrName : expected.getAttributeNames()) {
			assertThat(expected.getAttribute(attrName)).isEqualTo(expected.getAttribute(attrName));
		}
	}

	@RedisHash("spring:session:sessions")
	public static class RedisSession extends MapSession {

		@Id
		public String getId() {
			return super.getId();
		}
	}

	@Configuration
	@EnableRedisHttpSession
	static class Config {
		@Bean
		public JedisConnectionFactory connectionFactory() throws Exception {
			JedisConnectionFactory factory = new JedisConnectionFactory();
			factory.setUsePool(false);
			return factory;
		}
	}
}
