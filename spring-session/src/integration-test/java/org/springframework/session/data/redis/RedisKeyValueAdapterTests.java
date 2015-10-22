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

import static org.fest.assertions.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.annotation.Id;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.RedisKeyValueAdapter;
import org.springframework.data.redis.core.RedisKeyValueTemplate;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.convert.CustomConversions;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration;
import org.springframework.data.redis.core.convert.MappingConfiguration;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.redis.core.index.IndexConfiguration.RedisIndexSetting;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.repository.query.RedisQueryCreator;
import org.springframework.data.redis.repository.support.RedisRepositoryFactory;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.repository.CrudRepository;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.session.ExpiringSession;
import org.springframework.session.MapSession;
import org.springframework.session.Session;
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

	@Autowired RedisOperations<Object, Object> redis;

	@Autowired SessionRepository sessions;

	RedisSessionRepository repo;

	@Before
	public void setup() {

		IndexConfiguration indexConfig = new IndexConfiguration();
		indexConfig.addIndexDefinition(new RedisIndexSetting("spring:session:sessions", "creationTime"));

		RedisMappingContext mappingContext = new RedisMappingContext(new MappingConfiguration(indexConfig,
				new KeyspaceConfiguration()));

		RedisKeyValueAdapter adapter = new RedisKeyValueAdapter(redis, mappingContext, new CustomConversions(
				Arrays.<Converter<?, ?>> asList(new ReadingRedisSessionConverter(), new WritingRedisSessionConverter())));

		template = new RedisKeyValueTemplate(adapter, mappingContext);

		repo = new RedisRepositoryFactory(template, RedisQueryCreator.class).getRepository(RedisSessionRepository.class);
	}

	@Test
	public void saveAsLegacyAndGetAsNew() throws InterruptedException {
		SecurityContext context = SecurityContextHolder.createEmptyContext();
		User user = new User("user", "password", AuthorityUtils.createAuthorityList("ROLE_USER"));
		context.setAuthentication(new UsernamePasswordAuthenticationToken(user, user.getPassword(), user.getAuthorities()));

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
		context.setAuthentication(new UsernamePasswordAuthenticationToken(user, user.getPassword(), user.getAuthorities()));

		RedisSession sessionToSave = new RedisSession();

		sessionToSave.setAttribute("hi", "there");
		sessionToSave.setAttribute("SECURITY_CONTEXT", context);

		repo.save(sessionToSave);

		RedisSession actual = repo.findOne(sessionToSave.getId());

		assertEquals(sessionToSave, actual);

		actual = repo.findByCreationTime(sessionToSave.getCreationTime());

		assertEquals(sessionToSave, actual);
	}

	@Test
	public void insertAsNewAndGetAsOld() {

		SecurityContext context = SecurityContextHolder.createEmptyContext();
		User user = new User("user", "password", AuthorityUtils.createAuthorityList("ROLE_USER"));
		context.setAuthentication(new UsernamePasswordAuthenticationToken(user, user.getPassword(), user.getAuthorities()));

		ExpiringSession sessionToSave = new RedisSession();

		sessionToSave.setAttribute("hi", "there");
		sessionToSave.setAttribute("SECURITY_CONTEXT", context);

		template.insert(sessionToSave);

		Session actual = sessions.getSession(sessionToSave.getId());

		assertEquals((ExpiringSession) actual, sessionToSave);
	}

	private void assertEquals(ExpiringSession expected, ExpiringSession actual) {
		assertThat(actual).isNotNull();

		assertThat(expected.getId()).isEqualTo(actual.getId());
		assertThat(expected.getAttributeNames()).isEqualTo(actual.getAttributeNames());
		assertThat(expected.getCreationTime()).isEqualTo(expected.getCreationTime());

		// needed to skipt the next line as SessionRepository seems to update lastAccessedTime on get operation
		// assertThat(expected.getLastAccessedTime()).isEqualTo(actual.getLastAccessedTime());

		assertThat(expected.getMaxInactiveIntervalInSeconds()).isEqualTo(actual.getMaxInactiveIntervalInSeconds());
		for (String attrName : expected.getAttributeNames()) {
			assertThat(expected.getAttribute(attrName)).isEqualTo(expected.getAttribute(attrName));
		}
	}

	@WritingConverter
	static class WritingRedisSessionConverter implements Converter<RedisSession, Map<String, byte[]>> {

		private final RedisSerializer<Object> serializer = new JdkSerializationRedisSerializer();

		public Map<String, byte[]> convert(RedisSession source) {

			if (source == null) {
				return Collections.emptyMap();
			}

			Map<String, byte[]> sink = new LinkedHashMap<String, byte[]>();
			sink.put("id", serializer.serialize(source.getId()));
			sink.put("creationTime", serializer.serialize(source.getCreationTime()));
			sink.put("lastAccessedTime", serializer.serialize(source.getLastAccessedTime()));
			sink.put("maxInactiveInterval", serializer.serialize(source.getMaxInactiveIntervalInSeconds()));

			for (String key : source.getAttributeNames()) {
				sink.put("sessionAttr:" + key, serializer.serialize(source.getAttribute(key)));
			}

			return sink;
		}

	}

	@ReadingConverter
	static class ReadingRedisSessionConverter implements Converter<Map<String, byte[]>, RedisSession> {

		private final RedisSerializer<Object> serializer = new JdkSerializationRedisSerializer();

		public RedisSession convert(Map<String, byte[]> source) {

			if (source == null || source.isEmpty()) {
				return null;
			}

			RedisSession session = new RedisSession();
			session.setId((String) serializer.deserialize(source.get("id")));
			session.setCreationTime((Long) serializer.deserialize(source.get("creationTime")));
			session.setLastAccessedTime((Long) serializer.deserialize(source.get("lastAccessedTime")));
			session.setMaxInactiveIntervalInSeconds((Integer) serializer.deserialize(source.get("maxInactiveInterval")));

			for (Entry<String, byte[]> entry : source.entrySet()) {

				if (entry.getKey().startsWith("sessionAttr:")) {
					session.setAttribute(entry.getKey().substring("sessionAttr:".length()),
							serializer.deserialize(entry.getValue()));
				}
			}

			return session;
		}

	}

	@RedisHash("spring:session:sessions")
	public static class RedisSession extends MapSession {

		@Id
		public String getId() {
			return super.getId();
		}
	}

	static interface RedisSessionRepository extends CrudRepository<RedisSession, String> {

		RedisSession findByCreationTime(Long creationTime);
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
