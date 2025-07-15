# Copyright 2017 Palantir Technologies, Inc.
import functools
import json
import os

import pyspark
from py4j import java_gateway, protocol

from transforms._utils import is_spark_driver, memoized

#: Globally shared _gateway. This can be reused between Spark tasks.
_gateway = None


class JavaProxy(object):
    """A proxy object designed to wrap a Py4J proxy object.

    The purpose of this class is to allow subclasses to add/override methods
    exposed on the Java object.
    """

    #: Reuse a JVM view for the instance
    __jvm = None

    def __init__(self, proxy):
        """Create a JavaProxy object.

        Args:
            proxy (py4j.JavaObject): The object to wrap.
        """
        self._proxy = proxy

    @property
    def _gateway(self):
        return get_or_create_gateway()

    @property
    def _jvm(self):
        if not self.__jvm:
            self.__jvm = self._gateway.new_jvm_view()
        return self.__jvm


class JavaEnum(JavaProxy):
    """A proxy object designed to wrap a Py4J Java Enum."""

    def get(self, value):
        """Get the enum value for the given value."""
        if not hasattr(self._proxy, value):
            raise ValueError(f"Unknown {self.__class__.__name__}: {value}")
        return getattr(self._proxy, value)


@memoized
def object_mapper():
    return (
        get_or_create_gateway().jvm.com.palantir.conjure.java.serialization.ObjectMappers.newClientObjectMapper()
    )


def to_auth_header(auth_header):
    """Convert a Python `str` to a Java `com.palantir.tokens.auth.AuthHeader`.

    Args:
        auth_header (str): The auth header string.

    Returns:
        j.com.palantir.tokens.auth.AuthHeader
    """
    _jvm = get_or_create_gateway().jvm
    return _jvm.com.palantir.tokens.auth.AuthHeader.valueOf(auth_header)


def to_temp_creds_token(temp_creds_token):
    """Convert a Python `str` to a Java `java.util.`Optional` of
    `com.palantir.foundry.keyservice.api.TemporaryCredentialsAuthToken`.

    Args:
        temp_creds_token (str): The temporary credentials string.

    Returns:
        j.com.java.util.Optional of com.palantir.foundry.keyservice.api.TemporaryCredentialsAuthToken
    """
    _jvm = get_or_create_gateway().jvm
    return to_optional(
        temp_creds_token,
        _jvm.com.palantir.foundry.keyservice.api.TemporaryCredentialsAuthToken.of,
    )


def to_guava_optional(value, func=None):
    """Convert a Python value to a `com.google.common.base.Optional` using the given function.

    Args:
        value (any): The Python value to convert.
        func (any -> j.Object, optional): Conversion function from the Python value to a Java object.

    Note:
        If the Python value is `None` then `Optional#absent` is returned.

    Returns:
        j.com.google.common.base.Optional
    """
    if value is None:
        return get_or_create_gateway().jvm.com.google.common.base.Optional.absent()
    jobj = func(value) if func else value
    return get_or_create_gateway().jvm.com.google.common.base.Optional.of(jobj)


def to_hadoop_path(path):
    """Convert a Python `str` to a Java `org.apache.hadoop.fs.Path`.

    Args:
        path (str): The HDFS path.

    Returns:
        j.org.apache.hadoop.fs.Path
    """
    return get_or_create_gateway().jvm.org.apache.hadoop.fs.Path(path)


def to_list(value, func=None):
    """Convert a Python value to a `java.util.List`.

    Args:
        value (list or set): The Python value to convert.
        func (any -> j.Object, optional): Conversion function from the Python value to a Java object applied on each
        element of the iterable passed in input

    Returns:
        j.java.util.List
    """
    if not isinstance(value, list):
        raise TypeError("Expected list but found " + str(type(value)))

    value = [func(x) for x in value] if func is not None else value

    # Avoid Immutables because of https://github.com/bartdag/py4j/issues/280
    return get_or_create_gateway().jvm.com.google.common.collect.Lists.newArrayList(
        value
    )


def to_string_array(python_list):
    """Convert a Python list of strings to a java array of strings.

    Args:
        python_list (list): The Python list to convert.
    Returns:
        j.String[]
    """
    if not isinstance(python_list, list):
        raise TypeError("Expected list but found " + str(type(python_list)))

    gateway = get_or_create_gateway()
    java_array = gateway.new_array(gateway.jvm.String, len(python_list))
    for i, value in enumerate(python_list):
        java_array[i] = value
    return java_array


def from_optional(optional, func=None):
    """Extract a `java.util.Optional` to a Python value using the given function.
    Args:
        optional (Optional): The Java optional to extract.
        func (j.Object -> any, optional): Conversion function from the Java object to a Python value.
    Note:
        If the Java Optional is `Optional#empty` then `None` is returned.
    Returns:
        any
    """
    if optional.isEmpty():
        return None
    return func(optional.get()) if func is not None else optional.get()


def to_optional(value, func=None):
    """Convert a Python value to a `java.util.Optional` using the given function.

    Args:
        value (any): The Python value to convert.
        func (any -> j.Object, optional): Conversion function from the Python value to a Java object.

    Note:
        If the Python value is `None` then `Optional#empty` is returned.

    Returns:
        j.java.util.Optional
    """
    if value is None:
        return get_or_create_gateway().jvm.java.util.Optional.empty()
    jobj = func(value) if func else value
    return get_or_create_gateway().jvm.java.util.Optional.of(jobj)


def to_optional_int(value):
    """Convert a Python value to a `java.util.OptionalInt` using the given function.

    Args:
        value (any): The Python integer to convert.

    Note:
        If the Python value is `None` then `Optional#empty` is returned.

    Returns:
        j.java.util.OptionalInt
    """
    if value is None:
        return get_or_create_gateway().jvm.java.util.OptionalInt.empty()
    return get_or_create_gateway().jvm.java.util.OptionalInt.of(value)


def to_path(path):
    """Convert a Python `str` to a Java `java.nio.file.Path`.

    Args:
        path (str): The path to convert.

    Returns:
        j.java.nio.file.Path
    """
    gateway = get_or_create_gateway()
    var_args = gateway.new_array(get_or_create_gateway().jvm.String, 0)
    return gateway.jvm.java.nio.file.Paths.get(path, var_args)


def to_dependency_type(dependency_type):
    """Convert a Python `str` to a Java `com.palantir.foundry.catalog.api.transactions.DependencyType`.

    Args:
        dependency_type (str): The dependency type string to convert.

    Returns:
        j.com.palantir.foundry.catalog.api.transactions.DependencyType
    """
    return get_or_create_gateway().jvm.com.palantir.foundry.catalog.api.transactions.DependencyType.valueOf(
        dependency_type
    )


def to_marking_id(marking_id):
    """Convert a Python `str` to a Java `com.palantir.foundry.catalog.api.MarkingId`.

    Args:
        marking_id (str): The marking id string to convert.

    Returns:
        j.com.palantir.foundry.catalog.api.MarkingId
    """
    return get_or_create_gateway().jvm.com.palantir.foundry.catalog.api.MarkingId.of(
        marking_id
    )


def to_multipass_marking_id(marking_id):
    """Convert a Python `str` to a Java `com.palantir.gatekeeper.api.markings.MarkingId`.

    Args:
        marking_id (str): The marking id string to convert.

    Returns:
        j.com.palantir.gatekeeper.api.markings.MarkingId
    """
    return (
        get_or_create_gateway().jvm.com.palantir.gatekeeper.api.markings.MarkingId.of(
            marking_id
        )
    )


def to_rid(rid):
    """Convert a Python `str` to a Java `com.palantir.ri.ResourceIdentifier`.

    Args:
        rid (str): The resource identifier string to convert.

    Returns:
        j.com.palantir.ri.ResourceIdentifier
    """
    return get_or_create_gateway().jvm.com.palantir.ri.ResourceIdentifier.valueOf(rid)


def to_set(iterable, func=None):
    """Convert a Python value to a `java.util.Set`.

    Args:
        iterable (list or set): The Python iterable to convert.
        func: a function to apply to each element of the iterable

    Returns:
        j.java.util.Set
    """

    if func is not None:
        iterable = [func(element) for element in iterable]

    # Avoid Immutables because of https://github.com/bartdag/py4j/issues/280
    return get_or_create_gateway().jvm.com.google.common.collect.Sets.newHashSet(
        iterable
    )


def to_instant(epoch_milli):
    """Convert an int representing milliseconds from the epoch of 1970-01-01T00:00:00Z to a `java.time.Instant`.

    Args:
        epoch_milli (int): the number of milliseconds from 1970-01-01T00:00:00Z

    Returns:
        j.java.time.Instant
    """
    return get_or_create_gateway().jvm.java.time.Instant.ofEpochMilli(epoch_milli)


def to_map(d, key_mapper=lambda x: x, value_mapper=lambda x: x):
    """Convert a Python dict to a `java.util.Map`.
    Args:
        d (dict): The Python dict to convert.
        key_mapper: The function to map the key.
        value_mapper: The function to map the value.
    Returns:
        j.java.util.Dict
    """
    m = get_or_create_gateway().jvm.com.google.common.collect.Maps.newHashMapWithExpectedSize(
        len(d)
    )
    for k, v in d.items():
        m.put(key_mapper(k), value_mapper(v))

    return m


def from_map(m, key_extractor=lambda x: x, value_extractor=lambda x: x):
    """Convert a `java.util.Map` to a Python dict.
    Args:
        m (Map): The Java map to convert.
        key_extractor: The function to extract the key.
        value_extractor: The function to extract the value.
    Returns:
        dict()
    """
    return {
        key_extractor(entry.getKey()): value_extractor(entry.getValue())
        for entry in m.entrySet()
    }


def to_transaction_ref(transaction_ref):
    """Convert a Python `str` to a Java `com.palantir.foundry.catalog.api.transactions.Ref`.

    Args:
        transaction_ref (str): The transaction ref string to convert.

    Returns:
        j.com.palantir.foundry.catalog.api.transactions.Ref
    """
    return get_or_create_gateway().jvm.com.palantir.foundry.catalog.api.transactions.Ref.valueOf(
        transaction_ref
    )


def to_create_organization_request(org_name):
    """Convert a Python `create_organization_request` to a
    Java `com.palantir.compass.api.request.CreateOrganizationRequest`.

    Args:
        auth_header: The auth header for the request.
        org_name: The name of the organization to create.

    Returns:
        com.palantir.compass.api.request.CreateOrganizationRequest
    """
    return get_or_create_gateway().jvm.com.palantir.transforms.python.test.OrganizationAndProjectUtils.createOrganizationRequest(
        org_name
    )


def to_create_project_request(org_rid, project_name):
    """Convert a Python `create_organization_request` to a Java `com.palantir.compass.api.request.CreateProjectRequest`.

    Args:
        auth_header: The auth header for the request.
        org_rid: The rid of the organization to create the project under.
        project_name: The name of the project to create.

    Returns:
        com.palantir.compass.api.request.CreateOrganizationRequest
    """
    return get_or_create_gateway().jvm.com.palantir.transforms.python.test.OrganizationAndProjectUtils.createProjectRequest(
        org_rid, project_name
    )


def to_resolved_version(version):
    """Convert a Python string to object `dict` to a Java `com.palantir.tables.api.ResolvedVersion`.

    Args:
        version: The string to object `dict` representing a resolved version of a table.

    Returns:
        com.palantir.tables.api.ResolvedVersion
    """
    return object_mapper().readValue(
        json.dumps(version),
        java_gateway.get_java_class(
            get_or_create_gateway().jvm.com.palantir.tables.api.ResolvedVersion
        ),
    )


def to_incremental_mode(incremental_mode):
    """Convert a Python string to a Java `com.palanitr.tables.api.IncrementalMode`.

    Args:
        incremental_mode: The string value of the incremental mode.

    Returns:
        com.palantir.tables.api.IncrementalMode
    """
    return get_or_create_gateway().jvm.com.palantir.tables.api.IncrementalMode.valueOf(
        incremental_mode
    )


def _render_safe_loggable_exception(py4j_error):
    """Render a Py4JJavaError when the exception is SafeLoggable."""
    gateway_client = py4j_error.java_exception._gateway_client
    answer = gateway_client.send_command(py4j_error.exception_cmd)
    stacktrace = protocol.get_return_value(answer, gateway_client, None, None)

    if not stacktrace:
        return stacktrace

    stacktrace_lines = stacktrace.splitlines(True)
    return f"{py4j_error.errmsg}: {''.join(stacktrace_lines)}"


def _patched_get_return_value(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except protocol.Py4JJavaError as exc:
            gateway = get_or_create_gateway()
            if java_gateway.is_instance_of(
                gateway, exc.java_exception, "com.palantir.logsafe.SafeLoggable"
            ):
                exc.__str__ = _render_safe_loggable_exception
            raise exc

    return wrapper


def _install_exception_handler():
    # By reading off protocol and setting on java_gateway, we can be idempotent
    original = protocol.get_return_value
    patched = _patched_get_return_value(original)
    java_gateway.get_return_value = patched


def get_or_create_gateway(driver=None):
    """Get or create the global gateway instance.

    Args:
        driver (bool, optional): Optionally override if we are the driver or
            not. By default we check the pyspark.files.SparkFiles._is_running_on_worker
            parameter.

    Returns:
        py4j.JavaGateway
    """
    if driver is None:
        driver = is_spark_driver()

    global _gateway  # pylint: disable=global-statement

    if _gateway is None:
        if driver:
            _gateway = pyspark.SparkContext.getOrCreate()._gateway
        else:
            _gateway = _launch_executor_gateway()

    _install_exception_handler()

    return _gateway


def _launch_executor_gateway():
    """Launch a new JVM and return a Gateway connection to it."""
    # If we're on K8s, then all jars are in $SPARK_HOME/jars because we put them there in our docker image
    # If we're on Yarn, then a CLASSPATH envvar is set for us
    classpath = (
        os.path.join(os.environ["SPARK_HOME"], "jars/*")
        if _is_on_k8s()
        else os.environ["CLASSPATH"]
    )

    java_path = (
        _get_and_expand_java_path() or None
    )  # If set to 'None', Py4J will default to running java from PATH

    java_port = java_gateway.launch_gateway(
        java_path=java_path,
        # TODO(mbakovic): Verify these redirects #1423
        redirect_stdout=None,  # Redirects to /dev/null
        redirect_stderr=None,  # Redirects to /dev/null
        classpath=classpath,
        daemonize_redirect=True,
        die_on_exit=True,
    )

    return java_gateway.JavaGateway(
        gateway_parameters=java_gateway.GatewayParameters(
            port=java_port, auto_convert=True, eager_load=True
        )
    )


def _get_and_expand_java_path():
    """Get `JAVA_HOME` environment variable and expand if necessary.

    If using a non-default JRE version, `JAVA_HOME` will be set to, e.g., `$JAVA_11_HOME`. In that case we need to
    expand to get the actual path. If `JAVA_HOME` isn't set, return None.
    """
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        java_home = os.path.expandvars(java_home)
        return os.path.join(java_home, "bin", "java")
    return None


def _is_on_k8s():
    """Returns whether or not we're running on Kubernetes.

    We check for existence of 'KUBERNETES_SERVICE_HOST' env var as per
    https://kubernetes.io/docs/concepts/services-networking/connect-applications-service/#accessing-the-service
    """
    return "KUBERNETES_SERVICE_HOST" in os.environ
