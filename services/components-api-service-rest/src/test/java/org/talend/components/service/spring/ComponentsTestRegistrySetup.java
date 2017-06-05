package org.talend.components.service.spring;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.talend.components.api.ComponentInstaller;
import org.talend.components.api.service.ComponentService;
import org.talend.components.api.service.common.ComponentServiceImpl;
import org.talend.components.api.service.common.DefinitionRegistry;
import org.talend.daikon.definition.service.DefinitionRegistryService;

@Configuration
public class ComponentsTestRegistrySetup {

    /** This class' logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger(ComponentsTestRegistrySetup.class);

    @Autowired
    private ApplicationContext context;

    private DefinitionRegistry registry;

    @Bean
    public ComponentService getComponentService() {
        return new ComponentServiceImpl(getComponentRegistry());
    }

    @Bean
    public DefinitionRegistryService getDefintionRegistryService() {
        return getComponentRegistry();
    }

    private DefinitionRegistry getComponentRegistry() {
        if (registry == null) {
            registry = new DefinitionRegistry();
            Map<String, ComponentInstaller> installers = context.getBeansOfType(ComponentInstaller.class);
            for (ComponentInstaller installer : installers.values()) {
                installer.install(registry);
                LOGGER.debug("{} installed in the registry", installer);
            }

            registry.lock();
        } // else registry already initialised
        return registry;
    }
}
