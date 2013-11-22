package mapreduce;

import java.util.Locale;
import java.util.ResourceBundle;

public class Constants {
    public static final String RESOURCE_NAME = "config";
    private static final ResourceBundle resource = ResourceBundle.getBundle(RESOURCE_NAME, Locale.getDefault());

    public static String getResource(String rname) { return resource.getString(rname); }

    ///////////////////

    public static final String JOB_TRACKER_REGISTRY_HOST="job_tracker.registry.host";
    public static final String REGISTRY_PORT="registry.port";
    public static final String JOB_TRACKER_SERVICE_NAME="job_tracker.service.name";

    public static final String CLIENT_HOST="client.host";
    public static final String CLIENT_SERVICE_NAME="client.service.name";

    public static final String HEARTBEAT_PERIOD="heart_beat.period";
    public static final String ALIVE_CYCLE="alive.cycle";
    public static final String RECUDER_CHECK_MAPPER_CYCLE="reducer.check.mapper.cycle";

    public static final String SYSTEM_TEMP_DIR="system.temp.dir";
    public static final String USER_CLASS_PATH="user.class_path";

    public static final String MAPPER_STDOUT_REDIRECT="mapper.stdout.redirect";
    public static final String MAPPER_STDERR_REDIRECT="mapper.stderr.redirect";
    public static final String REDUCER_STDOUT_REDIRECT="reducer.stdout.redirect";
    public static final String REDUCER_STDERR_REDIRECT="reducer.stderr.redirect";

    public static final String THREAD_POOL_SIZE="thread_pool.size";

    public static final String RMI_CODE_BASE="rmi.code.base";
}
