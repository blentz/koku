#!/usr/bin/env python3
#
# Copyright 2018 Red Hat, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
'''Prometheus Exporter for PostgreSQL.'''

import logging
import math
import os
import re
import time
from datetime import timedelta
from enum import Enum
from packaging.version import Version

import yaml
import psycopg2
from prometheus_client import start_http_server, Counter, Gauge, Summary
from prometheus_client.registry import CollectorRegistry

# set up logging
logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)

# TODO: use ConfigParse
LISTEN_PORT = '9817'
DATABASE_URI = 'postgresql://postgres:postgres@localhost:5432'
EXPORTER_TELEMETRY_PATH = '/metrics'
EXPORTER_SCRAPE_SETTINGS = False
EXPORTER_LABELS = os.environ.get("PG_EXPORTER_CONSTANT_LABELS")   # label key=value pairs
EXPORTER_EXTEND_QUERIES_PATH = os.environ.get("PG_EXPORTER_EXTEND_QUERY_PATH")
DISABLE_SETTINGS = bool(os.environ.get('PG_EXPORTER_DISABLE_DEFAULT_METRICS'))
DISABLE_METRICS = bool(os.environ.get('PG_EXPORTER_DISABLE_SETTINGS_METRICS'))

# Metric strings
NAMESPACE = 'pg'
EXPORTER = 'exporter'
STATIC_LABEL = 'static'
SERVER_LABEL = 'server'

# regex to identify postgresql version numbers
PG_VERSION_RE = re.compile(r'^\w+ ((\d+)(\.\d+)?(\.\d+)?)')
PG_MINIMUM_VERSION = "9.1.0"

# Prometheus collector registry
REGISTRY = CollectorRegistry(auto_describe=True)

# Describes how a queried row will be converted to Prometheus metric
class ColumnUsage(Enum):
    """ ColumnUsage enum. """
    DISCARD = 0
    LABEL = 1
    COUNTER = 2
    GAUGE = 3
    MAPPEDMETRIC = 4
    DURATION = 5


def parse_version_range(versions):
    """ Parse a version range.

    Args:
        versions (str) A comma-separated version range stating a
                version requirment. e.g. ">=1.0,<2.0" or ">3.1.2"
    """
    parsed = []
    rgx = re.compile(r'(\W+)([0-9\.]+)')
    for item in versions.split(','):
        oper, ver = rgx.match(item).groups()
        tup = tuple([oper, Version(ver)])
        parsed.append(tup)
    return parsed


class ColumnMap():
    """COLUMN_MAP is a representation of a prometheus descriptor map."""

    def __init__(self, usage=ColumnUsage.DISCARD,
                 description='No Description',
                 mapping=None,
                 supported_versions=f'>={PG_MINIMUM_VERSION}'):
        self.usage = usage
        self.description = description
        self.mapping = mapping if mapping else {}  # optional column mapping for MAPPEDMETRIC
        self.supported_versions = parse_version_range(supported_versions)

    # pylint: disable=eval-used
    def is_supported(self, version):
        """compare given version to supported_versions.

        Returns: bool
        """
        parsed = Version(version)
        for supp in self.supported_versions:
            if eval(f'{parsed} {supp[0]} {supp[1]}'):
                return True
        return False


# pylint: disable=too-few-public-methods
class MetricMapNamespace():
    """MetricMapNamespace groups metric maps under a shared set of labels."""
    def __init__(self, labels, column_mappings):
        self.labels = labels
        self.column_mappings = column_mappings # MetricMap


# pylint: disable=too-few-public-methods
class MetricMap():
    """ MetricMap stores the prometheus metric description.

    A given column will be mapped to this description by the collector.

    """
    def __init__(self, **kwargs):
        self.discard = kwargs.get('discard')
        self.value_type = kwargs.get('value_type')

        # Desc
        self.metric_name = kwargs.get('metric_name')
        self.description = kwargs.get('description')
        self.variable_labels = kwargs.get('variable_labels')
        self.server_labels = kwargs.get('server_labels')

        self.conversion = kwargs.get('conversion') # conversion function. may be unnecessary.


# pylint: disable=line-too-long, bad-whitespace, bad-continuation
BUILTIN_METRIC_MAPS = {
        "pg_stat_bgwriter": {
                "checkpoints_timed": ColumnMap(ColumnUsage.COUNTER, description="Number of scheduled checkpoints that have been performed"),
                "checkpoints_req": ColumnMap(ColumnUsage.COUNTER, description="Number of requested checkpoints that have been performed"),
                "checkpoint_write_time": ColumnMap(ColumnUsage.COUNTER, description="Total amount of time that has been spent in the portion of checkpoint processing where files are written to disk, in milliseconds"),
                "checkpoint_sync_time":  ColumnMap(ColumnUsage.COUNTER, description="Total amount of time that has been spent in the portion of checkpoint processing where files are synchronized to disk, in milliseconds"),
                "buffers_checkpoint":    ColumnMap(ColumnUsage.COUNTER, description="Number of buffers written during checkpoints"),
                "buffers_clean":         ColumnMap(ColumnUsage.COUNTER, description="Number of buffers written by the background writer"),
                "maxwritten_clean":      ColumnMap(ColumnUsage.COUNTER, description="Number of times the background writer stopped a cleaning scan because it had written too many buffers"),
                "buffers_backend":       ColumnMap(ColumnUsage.COUNTER, description="Number of buffers written directly by a backend"),
                "buffers_backend_fsync": ColumnMap(ColumnUsage.COUNTER, description="Number of times a backend had to execute its own fsync call (normally the background writer handles those even when the backend does its own write)"),
                "buffers_alloc":         ColumnMap(ColumnUsage.COUNTER, description="Number of buffers allocated"),
                "stats_reset":           ColumnMap(ColumnUsage.COUNTER, description="Time at which these statistics were last reset"),
        },
        "pg_stat_database": {
                "datid":          ColumnMap(ColumnUsage.LABEL, description="OID of a database"),
                "datname":        ColumnMap(ColumnUsage.LABEL, description="Name of this database"),
                "numbackends":    ColumnMap(ColumnUsage.GAUGE, description="Number of backends currently connected to this database. This is the only column in this view that returns a value reflecting current state; all other columns return the accumulated values since the last reset."),
                "xact_commit":    ColumnMap(ColumnUsage.COUNTER, description="Number of transactions in this database that have been committed"),
                "xact_rollback":  ColumnMap(ColumnUsage.COUNTER, description="Number of transactions in this database that have been rolled back"),
                "blks_read":      ColumnMap(ColumnUsage.COUNTER, description="Number of disk blocks read in this database"),
                "blks_hit":       ColumnMap(ColumnUsage.COUNTER, description="Number of times disk blocks were found already in the buffer cache, so that a read was not necessary (this only includes hits in the PostgreSQL buffer cache, not the operating system's file system cache)"),
                "tup_returned":   ColumnMap(ColumnUsage.COUNTER, description="Number of rows returned by queries in this database"),
                "tup_fetched":    ColumnMap(ColumnUsage.COUNTER, description="Number of rows fetched by queries in this database"),
                "tup_inserted":   ColumnMap(ColumnUsage.COUNTER, description="Number of rows inserted by queries in this database"),
                "tup_updated":    ColumnMap(ColumnUsage.COUNTER, description="Number of rows updated by queries in this database"),
                "tup_deleted":    ColumnMap(ColumnUsage.COUNTER, description="Number of rows deleted by queries in this database"),
                "conflicts":      ColumnMap(ColumnUsage.COUNTER, description="Number of queries canceled due to conflicts with recovery in this database. (Conflicts occur only on standby servers; see pg_stat_database_conflicts for details.)"),
                "temp_files":     ColumnMap(ColumnUsage.COUNTER, description="Number of temporary files created by queries in this database. All temporary files are counted, regardless of why the temporary file was created (e.g., sorting or hashing), and regardless of the log_temp_files setting."),
                "temp_bytes":     ColumnMap(ColumnUsage.COUNTER, description="Total amount of data written to temporary files by queries in this database. All temporary files are counted, regardless of why the temporary file was created, and regardless of the log_temp_files setting."),
                "deadlocks":      ColumnMap(ColumnUsage.COUNTER, description="Number of deadlocks detected in this database"),
                "blk_read_time":  ColumnMap(ColumnUsage.COUNTER, description="Time spent reading data file blocks by backends in this database, in milliseconds"),
                "blk_write_time": ColumnMap(ColumnUsage.COUNTER, description="Time spent writing data file blocks by backends in this database, in milliseconds"),
                "stats_reset":    ColumnMap(ColumnUsage.COUNTER, description="Time at which these statistics were last reset"),
        },
        "pg_stat_database_conflicts": {
                "datid":            ColumnMap(ColumnUsage.LABEL, description="OID of a database"),
                "datname":          ColumnMap(ColumnUsage.LABEL, description="Name of this database"),
                "confl_tablespace": ColumnMap(ColumnUsage.COUNTER, description="Number of queries in this database that have been canceled due to dropped tablespaces"),
                "confl_lock":       ColumnMap(ColumnUsage.COUNTER, description="Number of queries in this database that have been canceled due to lock timeouts"),
                "confl_snapshot":   ColumnMap(ColumnUsage.COUNTER, description="Number of queries in this database that have been canceled due to old snapshots"),
                "confl_bufferpin":  ColumnMap(ColumnUsage.COUNTER, description="Number of queries in this database that have been canceled due to pinned buffers"),
                "confl_deadlock":   ColumnMap(ColumnUsage.COUNTER, description="Number of queries in this database that have been canceled due to deadlocks"),
        },
        "pg_locks": {
                "datname": ColumnMap(ColumnUsage.LABEL, description="Name of this database"),
                "mode":    ColumnMap(ColumnUsage.LABEL, description="Type of Lock"),
                "count":   ColumnMap(ColumnUsage.GAUGE, description="Number of locks"),
        },
        "pg_stat_replication": {
                "procpid":          ColumnMap(ColumnUsage.DISCARD, description="Process ID of a WAL sender process", supported_versions="<9.2.0"),
                "pid":              ColumnMap(ColumnUsage.DISCARD, description="Process ID of a WAL sender process", supported_versions=">=9.2.0"),
                "usesysid":         ColumnMap(ColumnUsage.DISCARD, description="OID of the user logged into this WAL sender process"),
                "usename":          ColumnMap(ColumnUsage.DISCARD, description="Name of the user logged into this WAL sender process"),
                "application_name": ColumnMap(ColumnUsage.DISCARD, description="Name of the application that is connected to this WAL sender"),
                "client_addr":      ColumnMap(ColumnUsage.LABEL, description="IP address of the client connected to this WAL sender. If this field is null, it indicates that the client is connected via a Unix socket on the server machine."),
                "client_hostname":  ColumnMap(ColumnUsage.DISCARD, description="Host name of the connected client, as reported by a reverse DNS lookup of client_addr. This field will only be non-null for IP connections, and only when log_hostname is enabled."),
                "client_port":      ColumnMap(ColumnUsage.DISCARD, description="TCP port number that the client is using for communication with this WAL sender, or -1 if a Unix socket is used"),
                "backend_start": ColumnMap(ColumnUsage.DISCARD, description="with time zone     Time when this process was started, i.e., when the client connected to this WAL sender"),
                "backend_xmin":             ColumnMap(ColumnUsage.DISCARD, description="The current backend's xmin horizon."),
                "state":                    ColumnMap(ColumnUsage.LABEL, description="Current WAL sender state"),
                "sent_location":            ColumnMap(ColumnUsage.DISCARD, description="Last transaction log position sent on this connection", supported_versions="<10.0.0"),
                "write_location":           ColumnMap(ColumnUsage.DISCARD, description="Last transaction log position written to disk by this standby server", supported_versions="<10.0.0"),
                "flush_location":           ColumnMap(ColumnUsage.DISCARD, description="Last transaction log position flushed to disk by this standby server", supported_versions="<10.0.0"),
                "replay_location":          ColumnMap(ColumnUsage.DISCARD, description="Last transaction log position replayed into the database on this standby server", supported_versions="<10.0.0"),
                "sent_lsn":                 ColumnMap(ColumnUsage.DISCARD, description="Last transaction log position sent on this connection", supported_versions=">=10.0.0"),
                "write_lsn":                ColumnMap(ColumnUsage.DISCARD, description="Last transaction log position written to disk by this standby server", supported_versions=">=10.0.0"),
                "flush_lsn":                ColumnMap(ColumnUsage.DISCARD, description="Last transaction log position flushed to disk by this standby server", supported_versions=">=10.0.0"),
                "replay_lsn":               ColumnMap(ColumnUsage.DISCARD, description="Last transaction log position replayed into the database on this standby server", supported_versions=">=10.0.0"),
                "sync_priority":            ColumnMap(ColumnUsage.DISCARD, description="Priority of this standby server for being chosen as the synchronous standby"),
                "sync_state":               ColumnMap(ColumnUsage.DISCARD, description="Synchronous state of this standby server"),
                "slot_name":                ColumnMap(ColumnUsage.LABEL, description="A unique, cluster-wide identifier for the replication slot", supported_versions=">=9.2.0"),
                "plugin":                   ColumnMap(ColumnUsage.DISCARD, description="The base name of the shared object containing the output plugin this logical slot is using, or null for physical slots"),
                "slot_type":                ColumnMap(ColumnUsage.DISCARD, description="The slot type - physical or logical"),
                "datoid":                   ColumnMap(ColumnUsage.DISCARD, description="The OID of the database this slot is associated with, or null. Only logical slots have an associated database"),
                "database":                 ColumnMap(ColumnUsage.DISCARD, description="The name of the database this slot is associated with, or null. Only logical slots have an associated database"),
                "active":                   ColumnMap(ColumnUsage.DISCARD, description="True if this slot is currently actively being used"),
                "active_pid":               ColumnMap(ColumnUsage.DISCARD, description="Process ID of a WAL sender process"),
                "xmin":                     ColumnMap(ColumnUsage.DISCARD, description="The oldest transaction that this slot needs the database to retain. VACUUM cannot remove tuples deleted by any later transaction"),
                "catalog_xmin":             ColumnMap(ColumnUsage.DISCARD, description="The oldest transaction affecting the system catalogs that this slot needs the database to retain. VACUUM cannot remove catalog tuples deleted by any later transaction"),
                "restart_lsn":              ColumnMap(ColumnUsage.DISCARD, description="The address (LSN) of oldest WAL which still might be required by the consumer of this slot and thus won't be automatically removed during checkpoints"),
                "pg_current_xlog_location": ColumnMap(ColumnUsage.DISCARD, description="pg_current_xlog_location"),
                "pg_current_wal_lsn":       ColumnMap(ColumnUsage.DISCARD, description="pg_current_xlog_location", supported_versions=">=10.0.0"),
                "pg_xlog_location_diff":    ColumnMap(ColumnUsage.GAUGE, description="Lag in bytes between master and slave", supported_versions=">=9.2.0,<10.0.0"),
                "pg_wal_lsn_diff":          ColumnMap(ColumnUsage.GAUGE, description="Lag in bytes between master and slave", supported_versions=">=10.0.0"),
                "confirmed_flush_lsn":      ColumnMap(ColumnUsage.DISCARD, description="LSN position a consumer of a slot has confirmed flushing the data received"),
                "write_lag":                ColumnMap(ColumnUsage.DISCARD, description="Time elapsed between flushing recent WAL locally and receiving notification that this standby server has written it (but not yet flushed it or applied it). This can be used to gauge the delay that synchronous_commit level remote_write incurred while committing if this server was configured as a synchronous standby.", supported_versions=">=10.0.0"),
                "flush_lag":                ColumnMap(ColumnUsage.DISCARD, description="Time elapsed between flushing recent WAL locally and receiving notification that this standby server has written and flushed it (but not yet applied it). This can be used to gauge the delay that synchronous_commit level remote_flush incurred while committing if this server was configured as a synchronous standby.", supported_versions=">=10.0.0"),
                "replay_lag":               ColumnMap(ColumnUsage.DISCARD, description="Time elapsed between flushing recent WAL locally and receiving notification that this standby server has written, flushed and applied it. This can be used to gauge the delay that synchronous_commit level remote_apply incurred while committing if this server was configured as a synchronous standby.", supported_versions=">=10.0.0"),
        },
        "pg_stat_activity": {
                "datname":         ColumnMap(ColumnUsage.LABEL, description="Name of this database"),
                "state":           ColumnMap(ColumnUsage.LABEL, description="connection state",  supported_versions=">=9.2.0"),
                "count":           ColumnMap(ColumnUsage.GAUGE, description="number of connections in this state"),
                "max_tx_duration": ColumnMap(ColumnUsage.GAUGE, description="max duration in seconds any active transaction has been running")
        }
}


class OverrideQuery():
    """ OverrideQuery 's are run in place of namespace look-ups, and provide advanced functionality.

    But they have a tendency to postgres version specific.

    There aren't too many versions, so we simply store customized versions using
    the same version matching we do for columns.

    """

    def __init__(self, query, version_range):
        """ Constructor.

        Args:
            query (str) A SQL Query.

            version_range (str) A comma-separated version range stating a
                    version requirment. e.g. ">=1.0,<2.0" or ">3.1.2"

        """
        self._query = query
        self._version_range = parse_version_range(version_range)

    @property
    def query(self):
        """Query property."""
        return self._query

    @property
    def version_range(self):
        """Version range property."""
        return self._version_range


QUERY_OVERRIDES = {
    "pg_locks": [OverrideQuery(version_range=">0.0.0", query="""
                    SELECT pg_database.datname,tmp.mode,COALESCE(count,0) as count
                    FROM ( VALUES ('accesssharelock'),
                                  ('rowsharelock'),
                                  ('rowexclusivelock'),
                                  ('shareupdateexclusivelock'),
                                  ('sharelock'),
                                  ('sharerowexclusivelock'),
                                  ('exclusivelock'),
                                  ('accessexclusivelock')
                         ) AS tmp(mode) CROSS JOIN pg_database
                    LEFT JOIN (
                      SELECT database, lower(mode) AS mode,count(*) AS count
                       FROM pg_locks WHERE database IS NOT NULL
                       GROUP BY database, lower(mode)
                    ) AS tmp2
                    ON tmp.mode=tmp2.mode and pg_database.oid = tmp2.database ORDER BY 1
                """)],
    "pg_stat_replication": [OverrideQuery(version_range=">=10.0.0", query="""
                                SELECT *,
                                (case pg_is_in_recovery() when 't' then null
                                    else pg_current_wal_lsn() end) AS pg_current_wal_lsn,
                                (case pg_is_in_recovery() when 't' then null
                                    else pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::float end) AS pg_wal_lsn_diff
                                FROM pg_stat_replication
                            """),
                            OverrideQuery(version_range=">=9.2.0 <10.0.0", query="""
                                SELECT *,
                                (case pg_is_in_recovery() when 't' then null
                                    else pg_current_xlog_location() end) AS pg_current_xlog_location,
                                (case pg_is_in_recovery() when 't' then null
                                    else pg_xlog_location_diff(pg_current_xlog_location(), replay_location)::float end) AS pg_xlog_location_diff
                                FROM pg_stat_replication
                            """),
                            OverrideQuery(version_range="<9.2.0", query="""
                                SELECT *,
                                (case pg_is_in_recovery() when 't' then null
                                    else pg_current_xlog_location() end) AS pg_current_xlog_location
                                FROM pg_stat_replication
                            """)],
    "pg_stat_activity": [OverrideQuery(version_range=">=9.2.0", query="""
                            SELECT
                                    pg_database.datname,
                                    tmp.state,
                                    COALESCE(count,0) as count,
                                    COALESCE(max_tx_duration,0) as max_tx_duration
                            FROM ( VALUES
                                ('active'),
                                ('idle'),
                                ('idle in transaction'),
                                ('idle in transaction (aborted)'),
                                ('fastpath function call'),
                                ('disabled')
                            ) AS tmp(state) CROSS JOIN pg_database
                            LEFT JOIN ( SELECT
                                datname,
                                state,
                                count(*) AS count,
                                MAX(EXTRACT(EPOCH FROM now() - xact_start))::float AS max_tx_duration
                            FROM pg_stat_activity GROUP BY datname,state) AS tmp2
                            ON tmp.state = tmp2.state AND pg_database.datname = tmp2.datname
                        """),
                        OverrideQuery(version_range="<9.2.0", query="""
                            SELECT
                                datname,
                                'unknown' AS state,
                                COALESCE(count(*),0) AS count,
                                COALESCE(MAX(EXTRACT(EPOCH FROM now() - xact_start))::float,0) AS max_tx_duration
                            FROM pg_stat_activity GROUP BY datname
                        """)]}


#TODO: implement addQueries()

def parse_duration(time_str):
    """Parse duration string into timedelta."""
    regex = re.compile(r'((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?((?P<milliseconds>\d+?)ms)?')
    parts = regex.match(time_str)
    if not parts:
        return None
    parts = parts.groupdict()
    time_params = {}
    for (name, param) in parts.iteritems():
        if param:
            time_params[name] = int(param)
    return timedelta(**time_params)


def make_queryoverride_map(pgversion, override_queries):
    """ Convert the query override file to the required form.

    Args:
        pgversion (str) A version number
        override_queries (dict) A dict of { name (str): [OverrideQuery,] }
    """
    results = {}
    for name, queries in override_queries:
        matched = False
        for _, query in queries:
            # pylint: disable=eval-used
            if eval(f'{pgversion} {query.version_range[0]} {query.version_range[1]}'):
                results[name] = query.query
                matched = True
                break

        if not matched:
            LOG.warning('No query matched override for %s - disabling metric space.',
                        name)


def make_descriptor_mapping(version, server_labels, metric_maps):
    """Turn the MetricMap column mapping into a prometheus descriptor mapping."""
    metric_map = {}

    for namespace, mappings in metric_maps:
        this_map = {}

        # collect constant labels
        variable_labels = []
        for column_name, column_mapping in mappings:
            if column_mapping.usage == ColumnUsage.LABEL:
                variable_labels.append(column_name)

        for column_name, column_mapping in mappings:
            # discard if versioning is incompatible
            if column_mapping.is_supported(version):
                LOG.debug('%s is discarded due to incompatible version', column_name)
                this_map[column_name] = MetricMap(discard=True,
                                                  conversion=lambda x: (math.nan, True))
                continue

            # this is what happens when Python doesn't have a switch/case
            if column_mapping.usage in [ColumnUsage.DISCARD, ColumnUsage.LABEL]:
                this_map[column_name] = MetricMap(discard=True,
                                                  conversion=lambda x: (math.nan, True))
            elif column_mapping.usage == ColumnUsage.COUNTER:
                this_map[column_name] = MetricMap(value_type=Counter,
                                                  metric_name=f'{namespace}_{column_name}',
                                                  description=column_mapping.description,
                                                  variable_labels=variable_labels,
                                                  server_labels=server_labels,
                                                  conversion=lambda x: (float(x), True))
            elif column_mapping.usage == ColumnUsage.GAUGE:
                this_map[column_name] = MetricMap(value_type=Gauge,
                                                  metric_name=f'{namespace}_{column_name}',
                                                  description=column_mapping.description,
                                                  variable_labels=variable_labels,
                                                  server_labels=server_labels,
                                                  conversion=lambda x: (float(x), True))
            elif column_mapping.usage == ColumnUsage.MAPPEDMETRIC:
                this_map[column_name] = MetricMap(value_type=Gauge,
                                                  metric_name=f'{namespace}_{column_name}',
                                                  description=column_mapping.description,
                                                  variable_labels=variable_labels,
                                                  server_labels=server_labels,
                                                  conversion=lambda x: (column_mapping.mapping.get(x), True) \
                                                                        if x in column_mapping.mapping \
                                                                        else (math.nan, False))
            elif column_mapping.usage == ColumnUsage.DURATION:
                def convert_duration(inval):
                    """Convert duration string into milliseconds."""
                    duration = ""
                    try:
                        duration = str(inval)
                    except ValueError:
                        LOG.error('DURATION metric "%s" did not convert to string', inval)
                        return (math.nan, False)

                    if duration == '-1':
                        return (math.nan, False)

                    milliseconds = parse_duration(duration).total_seconds() * 1000.0
                    return (milliseconds, True)

                this_map[column_name] = MetricMap(value_type=Gauge,
                                                  metric_name=f'{namespace}_{column_name}_milliseconds',
                                                  description=column_mapping.description,
                                                  variable_labels=variable_labels,
                                                  server_labels=server_labels,
                                                  conversion=convert_duration)
            else:
                LOG.error('Invalid usage "%s" for column "%s"',
                          column_mapping.usage, column_name)

        metric_map[namespace] = MetricMapNamespace(labels=variable_labels,
                                                   column_mappings=this_map)
    return metric_map


def dbquery(cursor, query, max_retries=3, sleep_interval=2):
    """ Execute a SQL query. """
    retries = 0
    rows = None
    while retries < max_retries:
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            LOG.warning(exc)
            retries += 1
            time.sleep(sleep_interval)
            dbconnect(DATABASE_URI)
            continue
        break

    if not rows:
        LOG.error('Query failed to return results.')
        return []  # FIXME: probably wrong
    return rows


def dbconnect(uri):
    """Connect to DB.

    Returns: tuple(connection, cursor)

    """
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(uri)
        cursor = connection.cursor()
    except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
        LOG.error(exc)
        raise exc
    return (connection, cursor)


# Create a metric to track time spent and requests made.
REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')


# pylint: disable=too-many-instance-attributes
class Exporter():
    """ Exporter collects Postgres metrics. """

    def __init__(self, dsn, **kwargs):
        """Constructor."""

        self.disable_default_metrics = kwargs.get('disable_default_metrics',
                                                  False)
        self.disable_settings_metrics = kwargs.get('disable_settings_metrics',
                                                   False)
        self.dsn = dsn
        self.user_queries_path = kwargs.get('user_queries_path', '')
        self.constant_labels = kwargs.get('constant_labels', {})

        # servers are used to allow re-using the DB connection between scrapes.
        # servers contains metrics map and query overrides.
        self.servers = {}

        self._setup_internal_metrics()
        self._setup_servers()

    def _setup_internal_metrics(self):
        """Configure internal metrics."""

        self.duration = Gauge("last_scrape_duration_seconds",
                              "Duration of the last scrape of metrics from PostgresSQL.",
                              namespace=NAMESPACE,
                              subsystem='exporter',
                              labelnames=self.constant_labels.keys(),
                              labelvalues=self.constant_labels.values())

        self.total_scrapes = Counter("scrapes_total",
                                     "Total number of times PostgresSQL was scraped for metrics.",
                                     namespace=NAMESPACE,
                                     subsystem='exporter',
                                     labelnames=self.constant_labels.keys(),
                                     labelvalues=self.constant_labels.values())

        self.error = Gauge("last_scrape_error",
                           "Whether the last scrape of metrics from PostgreSQL resulted in an error (1 for error, 0 for success).",
                           namespace=NAMESPACE,
                           subsystem='exporter',
                           labelnames=self.constant_labels.keys(),
                           labelvalues=self.constant_labels.values())

        self.psql_up = Gauge("up",
                            "Whether the last scrape of metrics from PostgreSQL was able to connect to the server (1 for yes, 0 for no).",
                            namespace=NAMESPACE,
                            labelnames=self.constant_labels.keys(),
                            labelvalues=self.constant_labels.values())

        self.user_queries_error = Gauge("user_queries_load_error",
                                        "Whether the user queries file was loaded and parsed successfully (1 for error, 0 for success).",
                                        namespace=NAMESPACE,
                                        subsystem='exporter',
                                        labelnames=self.constant_labels.keys(),
                                        labelvalues=self.constant_labels.values())

    def _setup_servers(self):
        """Connect to all DSNs."""

        for dsn in self.dsn:
            conn, curs = dbconnect(DATABASE_URI)
            self.servers[dsn] = {'connection': conn, 'cursor': curs}


def load_yaml(filename):
    """Load a YAML file."""
    try:
        yamlfile = yaml.load(open(filename, 'r+'))
    except TypeError:
        yamlfile = yaml.load(filename)
        # allow IOErrors to raise
    return yamlfile


def get_data_sources():
    """Obtain list of data sources from the environment.

    DATA_SOURCE_NAME always wins so we do not break older versions
    reading secrets from files wins over secrets in environment variables
    DATA_SOURCE_NAME > DATA_SOURCE_FILE > env DATA_SOURCE_{USER|PASS}

    """
    # TODO: use ConfigParse
    dsn = os.environ('DATA_SOURCE_NAME')
    user = None
    password = None

    if not dsn:
        user_file = os.environ.get('DATA_SOURCE_FILE')
        if user_file:
            data = load_yaml(user_file)
            user = data.get('user')
            password = data.get('password')
        else:
            user = os.environ.get('DATA_SOURCE_USER')
            password = os.environ.get('DATA_SOURCE_PASSWORD')
        uri = os.environ.get('DATA_SOURCE_URI')

        if user and password:
            dsn = 'postgresql://'+ user + ':' + password + '@' + uri
        else:
            dsn = 'postgresql://' + uri

    return dsn.split(',')


def parse_constant_labels(labels):
    """Parse comma-separated key=value pairs into a dict."""

    if labels.isspace():
        return {}

    parsed = {}
    llist = labels.strip().split(',')
    for label in llist:
        try:
            key, val = label.split('=')
        except ValueError:
            LOG.error('Wrong constant labels format %s, should be "key=value"',
                      label)
            continue

        if key.isspace() or val.isspace():
            LOG.warning('Skipping "%s" due to empty key or value.', label)
            continue

        parsed[key.strip()] = val.strip()
        return parsed


#TODO: implement queryNamespaceMapping
#TODO: implement queryNamespaceMappings
#TODO: implement checkMapVersions
#TODO: implement Scrape
#TODO: implement scrapeDSN


# Decorator: request processing metric.
@REQUEST_TIME.time()
def process_request():
    """Process metrics requests."""
    LOG.debug('processing request...')
    # TODO: scrape metrics here

    dsn = get_data_sources()
    if not dsn:
        LOG.critical('could not find datasource environment variable(s)')
        return False

    opts = {'disable_default_metrics': DISABLE_METRICS,
            'disable_settings_metrics': DISABLE_SETTINGS,
            'user_queries_path': EXPORTER_EXTEND_QUERIES_PATH,
            'constant_labels': parse_constant_labels(EXPORTER_LABELS)}
    exporter = Exporter(dsn, **opts)

    return True


if __name__ == '__main__':
    # TODO: register metrics
    start_http_server(LISTEN_PORT, registry=REGISTRY)
    PROCESSING = True
    while PROCESSING:
        PROCESSING = process_request()
