import monitoring
import requests

from pyutils import EnvObject
env = EnvObject()

interfaces = {
    "sicc": (
        "dcampcer",
        "dgeografia",
        "debelista",
        "debelistadatosadic",
        "dmatrizcampana",
        "dgeografiacampana",
        "dletsrangoscomision",
        "dnrodocumento",
        "fvtaproebecam",
        "fstaebecam",
        "fstaebeadic",
        "tstaebecam",
        "dstatusf",
        "dtiempoactividadzona",
        "fnumpedcam",
        "dapoyoproducto"
    ),
    "planit": (
        "dcontrol",
        "dtipooferta",
        "dmatrizcampana",
        "dcostoproductocampana",
        "fnumpedcam",
        "fvtaprocammes"
    ),
    "sap": (
        "dproducto"
    ),
    "bi": (
        "debelista",
        "findsociaemp",
        "dcatalogovehiculo",
        "fstaebecam",
        "dcomportamientorolling",
        "dstatus",
        "fresultadopalancas",
        "dpais",
        "dpalancas"
    ),
    "digital": (
        "flogingresoportal",
        "fingresosconsultoraportal",
        "dorigenpedidoweb",
        "fpedidowebdetalle",
        "fofertafinalconsultora",
        "fcompdigcon"
    )
}


map_steps = {
    'initial': 'load_parquet_partition',
    'load_parquet_partition': 'load_redshift_partition',
    'load_redshift_partition': 'load_functional',
    'load_functional': 'load_functional_redshift',
    'load_functional_redshift': 'final'
}


def get_jdbc():
    return "".join([
        "jdbc:redshift://", env.endpoint_address, ":", env.endpoint_port, "/",
        env.redshift_db, "?user=", env.redshift_user,
        "&password=", env.redshift_password
    ])


def get_base_livy_payload(class_name):
    return {
        'file': env.jar_project,
        'className': f"{env.package_name}.{class_name}",
        'executorCores': int(env.exec_cores),
        'executorMemory': env.executor_memory,
        'driverMemory': env.driver_memory,
        'conf': {
            'spark.master': 'yarn',
            'spark.submit.deployMode': 'cluster',
            'spark.driver.extraJavaOptions': '-Xss2m',
            'spark.yarn.maxAppAttempts': '1'
        }
    }



def get_spark_command_partition(task_data, step, extra_args = None):
    args = [
        "--process-all", "--skip-failures", 
        "--glue-staging-database", env.glue_staging_db,
        "--glue-landing-database", env.glue_landing_db,
        "--glue-functional-database", env.glue_functional_db,
        "--system", task_data['system'],
        "--country", task_data['country'],
        "--year", task_data['year'], "--month", task_data['month'],
        "--day", task_data['day'], "--secs", task_data['secs'],
        "--execution-id", task_data['execution_id'],
        "--attempt", str(task_data['attempt']),
        "--tempS3Dir", env.s3_temp_dir, "--step-id", step,
        "--jdbc", get_jdbc(), "--lambda-callback", env.function_name,
        "--lock-table", env.emr_lock_table
    ]

    monitoring_settings = monitoring.MonitoringSettings.get_global()
    if monitoring_settings.monitoring_topic:
        args.extend(["--monitoring-topic", monitoring_settings.monitoring_topic])

    if extra_args:
        args.extend(extra_args)

    return args


def get_glue_table(glue, database_name, table_name):
    try:
        response = glue.get_table(DatabaseName=database_name, Name=table_name)
        return response['Table']
    except glue.exceptions.EntityNotFoundException:
        return False


def get_queue_name(prefix, env, project, fifo=True):
    suffix = '.fifo' if fifo else ''
    return f"{prefix}-{env}-{project}{suffix}"

# Step payload functions

def get_load_parquet_partition_request(payload):
    base = get_base_livy_payload('LoadToParquet')
    base['args'] = get_spark_command_partition(payload, 'load_parquet_partition')

    return base


def get_load_redshift_partition_request(payload):
    base = get_base_livy_payload('LoadOnRedshift')
    base['args'] = get_spark_command_partition(payload, 'load_redshift_partition')

    return base


def get_load_functional_request(payload):
    base = get_base_livy_payload('LoadModel')
    base['args'] = get_spark_command_partition(payload, 'load_functional',
        extra_args=["--create", "--update", "--model", env.model])

    return base


def get_load_functional_redshift_request(payload):
    base = get_base_livy_payload('LoadModelToParquet')
    base['args'] = get_spark_command_partition(payload, 'load_functional_redshift',
        extra_args=["--model", env.model])

    return base


step_processing = {
    'load_parquet_partition': get_load_parquet_partition_request,
    'load_redshift_partition': get_load_redshift_partition_request,
    'load_functional': get_load_functional_request,
    'load_functional_redshift': get_load_functional_redshift_request
}


def send_job(step, payload):
    body = step_processing[step](payload)

    address = f"http://{env.emr_master_address}:8998/batches"
    print(address)
    print(body)
    response = requests.post(address, json=body, headers={
        'Content-Type': 'application/json'
    })

    print(response.json())
    response.raise_for_status()
