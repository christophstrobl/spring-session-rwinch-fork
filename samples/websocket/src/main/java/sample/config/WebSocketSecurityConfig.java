/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package sample.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.SimpMessageType;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.security.config.annotation.web.messaging.MessageSecurityMetadataSourceRegistry;
import org.springframework.security.messaging.access.intercept.ChannelSecurityInterceptor;

/**
 * @author Rob Winch
 */
@Configuration
public class WebSocketSecurityConfig extends AbstractSecurityWebSocketMessageBrokerConfigurer {

    @Override
    protected void configureInbound(MessageSecurityMetadataSourceRegistry messages) {
        messages
            .antMatchers(SimpMessageType.MESSAGE,"/queue/**","/topic/**").denyAll()
            .antMatchers(SimpMessageType.SUBSCRIBE, "/queue/**/*-user*","/topic/**/*-user*").denyAll()
            .anyMessage().hasRole("USER");
    }

    @Override
    protected void configureOutbound(MessageSecurityMetadataSourceRegistry messages) {
        messages
            .antMatchers(SimpMessageType.MESSAGE,"/**").hasRole("USER");
    }



    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.configureBrokerChannel().setInterceptors(messageUserToSecurityContextChannelInterceptor(),securityContextToMessageUserChannelInterceptor());
    }


}