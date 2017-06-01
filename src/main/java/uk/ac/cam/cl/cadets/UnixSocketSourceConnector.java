package uk.ac.cam.cl.cadets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.connector.Task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnixSocketSourceConnector extends SourceConnector {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(UnixSocketSourceConnector.class);

    private Map<String, String> configProps;

    @Override
    public ConfigDef config() {
        return UnixSocketSourceConfig.CONFIG_DEF;
    }

    @Override
    public void start(Map<String, String> props) {
        configProps = props;
    }

    @Override
    public void stop() {
    }

    @Override
    public Class<? extends Task> taskClass() {
        return UnixSocketSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        LOGGER.info("Setting task configurations for {} workers", maxTasks);
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(configProps);
        }
        return configs;
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
