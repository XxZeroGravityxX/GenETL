# Import modules
import logging

# Import third-party modules
import sqlalchemy  # noqa: F401  (re-exported for caller convenience)

# Import custom modules
from etl_tools.sql import (
    SQLALCHEMY_DTYPES,
    resolve_sqlalchemy_dtype,
    resolve_type_class,
    sql_exec_stmt,
    sql_read_data,
    sql_upload_data,
)


# Module-level logger
logger = logging.getLogger(__name__)


# Allowed connection type prefixes (kept here to centralise validation)
_ALLOWED_CONN_TYPES: frozenset[str] = frozenset(
    {
        "sqlalchemy",
        "pyodbc",
        "redshift",
        "oracledb",
        "mysql",
        "postgresql",
        "mssql",
        "bigquery",
        "cloudsql",
    }
)


def _render_stmt(stmt: str, extra_vars: dict | None) -> str:
    """Safely render a SQL template using ``str.format`` (no ``eval``).

    Only the variables provided in ``extra_vars`` are interpolated. Curly
    braces that do not match a provided key are left untouched.
    """
    if not isinstance(stmt, str):
        raise TypeError(f"SQL statement must be a string, got {type(stmt).__name__}")

    if not extra_vars:
        return stmt

    # Use str.format_map with a defaulting mapping so missing keys raise a
    # clear error instead of silently producing wrong SQL.
    class _StrictDict(dict):
        def __missing__(self, key):
            raise KeyError(
                f"Missing extra variable '{key}' for SQL statement templating"
            )

    return stmt.format_map(_StrictDict(extra_vars))


class ExtractDeleteAndLoad(object):
    """
    Routines to read, delete and/or upload data from/to a database or data
    storage backend.

    .. note::
        Compared to previous versions this class no longer uses ``eval()``.

        * ``sqlalchemy_dict`` maps alias names to SQLAlchemy types. Each value
          may be either a type class/callable (e.g. ``sqlalchemy.String``), a
          simple mapping key (e.g. ``"String"``) or a fully-qualified dotted
          path string rooted at ``sqlalchemy`` (e.g.
          ``"sqlalchemy.types.String"``). String values are resolved safely
          via :func:`etl_tools.sql.resolve_type_class` (no ``eval``/``exec``,
          allow-listed root only).
          A default mapping :data:`etl_tools.sql.SQLALCHEMY_DTYPES` is merged
          in (user-supplied keys take precedence).
        * Each ``<process>_extra_vars_dict`` must contain plain values. SQL
          templates with ``{var}`` placeholders are rendered with
          ``str.format`` instead of ``eval``.
        * ``globals_dict`` / ``locals_dict`` arguments were removed.
    """

    def __init__(
        self,
        config_dict: dict | None = None,
        conn_dict: dict | None = None,
        sqlalchemy_dict: dict | None = None,
    ):
        """
        Class constructor.

        Parameters:
            config_dict (dict | None): Configuration dictionary with connection
                and data parameters. May contain, for each process
                (``download``, ``delete``, ``truncate``, ``upload``):

                    * ``<process_name>_connections_dict``
                    * ``<process_name>_extra_vars_dict``
                    * ``<process_name>_sql_stmts_dict``
                    * ``<process_name>_tables_dict``
                    * ``<process_name>_schemas_dict``
                    * ``<process_name>_python_to_sql_dtypes_dict``
                    * ``<process_name>_custom_conn_strs_dict``
                    * ``<process_name>_connect_args_dict``
                    * ``<process_name>_chunksizes_dict``
                    * ``<process_name>_methods_dict``

            conn_dict (dict | None): Mapping ``<conn_type>_<conn_name> -> conn info``.
            sqlalchemy_dict (dict | None): User-supplied alias-to-type mapping
                for SQLAlchemy types. Values may be either a type class/callable,
                a simple mapping key (e.g. ``"String"``), or a dotted path
                string rooted at ``sqlalchemy`` (e.g.
                ``"sqlalchemy.types.String"``); string values are resolved
                via :func:`etl_tools.sql.resolve_type_class`. Merged on
                top of :data:`etl_tools.sql.SQLALCHEMY_DTYPES`.
        """
        config_dict = config_dict or {}
        conn_dict = conn_dict or {}
        sqlalchemy_dict = sqlalchemy_dict or {}

        # Normalise to lowercase keys
        self.connections_dict = {key.lower(): val for key, val in conn_dict.items()}
        self.configs_dict = {key.lower(): val for key, val in config_dict.items()}

        # Merge SQLAlchemy dtype mapping (user overrides defaults).
        #
        # Each user-supplied value can be:
        #   * a callable / type class -> stored as-is
        #   * a simple key (e.g. "String") -> looked up in SQLALCHEMY_DTYPES
        #   * a dotted path string under an allow-listed root (e.g.
        #     "sqlalchemy.types.String") -> resolved safely via
        #     resolve_type_class (no eval/exec).
        # Any other value is rejected.
        merged_dtypes: dict = dict(SQLALCHEMY_DTYPES)
        for k, v in sqlalchemy_dict.items():
            if isinstance(v, str):
                try:
                    resolved = resolve_type_class(v, mapping=SQLALCHEMY_DTYPES)
                except ValueError as exc:
                    raise ValueError(
                        f"sqlalchemy_dict['{k}'] = {v!r} could not be resolved "
                        f"to a SQLAlchemy type: {exc}"
                    ) from exc
                if not callable(resolved):
                    raise TypeError(
                        f"sqlalchemy_dict['{k}'] resolved from {v!r} is not a "
                        f"callable type, got {type(resolved).__name__}."
                    )
                merged_dtypes[k] = resolved
            elif callable(v):
                merged_dtypes[k] = v
            else:
                raise TypeError(
                    f"sqlalchemy_dict['{k}'] must be a SQLAlchemy type class, "
                    f"callable, or dotted-path string under "
                    f"'sqlalchemy.*'; got {type(v).__name__}."
                )
        self.sqlalchemy_dtypes = merged_dtypes

        # Process metadata
        processes_list = ["download", "delete", "truncate", "upload"]
        self.conn_info_dict: dict = {key: {} for key in processes_list}
        self.conn_suff_dict: dict = {key: {} for key in processes_list}
        self.conn_type_dict: dict = {key: {} for key in processes_list}

        # Build per-process connection metadata
        for p_name in processes_list:
            cfg_key = f"{p_name}_connections_dict"
            if cfg_key not in self.configs_dict:
                continue
            for key, val in self.configs_dict[cfg_key].items():
                parts = val.split("_")
                self.conn_suff_dict[p_name][key] = parts[-1]
                self.conn_type_dict[p_name][key] = parts[0]
                self.conn_info_dict[p_name][key] = self.connections_dict[val]

        # Validate connection types
        for process, type_map in self.conn_type_dict.items():
            for key, conn_type in type_map.items():
                if conn_type not in _ALLOWED_CONN_TYPES:
                    raise ValueError(
                        f"Invalid connection type '{conn_type}' for "
                        f"{process}:{key}. Allowed types are: "
                        f"{', '.join(sorted(_ALLOWED_CONN_TYPES))}"
                    )

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _require_process(self, process: str) -> None:
        if (
            process not in self.conn_suff_dict
            or process not in self.conn_type_dict
            or process not in self.conn_info_dict
        ):
            raise ValueError(
                f"{process.capitalize()} configurations are not set! "
                "Please set them first."
            )

    def _get_extra_vars(self, process: str, key: str) -> dict:
        """Return the extra-vars dict for a process/key, or ``{}``."""
        cfg_key = f"{process}_extra_vars_dict"
        return (self.configs_dict.get(cfg_key) or {}).get(key, {}) or {}

    def _kwargs_or_config(self, process: str, key: str, name: str,
                          default, kwargs: dict):
        """Resolve a per-call kwarg, falling back to per-key config, then default."""
        cfg_key = f"{process}_{name}s_dict"
        if name in kwargs:
            return kwargs[name]
        if cfg_key in self.configs_dict and key in self.configs_dict[cfg_key]:
            return self.configs_dict[cfg_key][key]
        return default

    def _build_dtypes_dict(self, key: str) -> dict:
        """Translate a column->dtype-spec dict to SQLAlchemy dtype instances."""
        col_dict = self.configs_dict["upload_python_to_sql_dtypes_dict"][key]
        return {
            col: resolve_sqlalchemy_dtype(spec, mapping=self.sqlalchemy_dtypes)
            for col, spec in col_dict.items()
        }

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def delete_data(self, **kwargs):
        """Delete data from each configured ``delete`` connection."""
        self._require_process("delete")
        for key in self.configs_dict["delete_connections_dict"].keys():
            logger.info(f"Deleting data for {key}...")
            extra_vars = self._get_extra_vars("delete", key)
            conn_type = self.conn_type_dict["delete"][key]
            conn_dict = self.conn_info_dict["delete"][key]
            raw_stmt = self.configs_dict["delete_sql_stmts_dict"][key]
            stmt = _render_stmt(raw_stmt, extra_vars)
            logger.info(f"     Delete query: {stmt}")
            try:
                sql_exec_stmt(
                    stmt,
                    conn_dict,
                    mode=kwargs.get("mode", conn_type),
                    **{k: v for k, v in kwargs.items() if k != "mode"},
                )
            except Exception as e:
                logger.error(f"Error deleting data: {type(e).__name__} - {e}")
                raise

    def truncate_data(self, **kwargs):
        """Truncate data from each configured ``truncate`` connection."""
        self._require_process("truncate")
        for key in self.configs_dict["truncate_connections_dict"].keys():
            logger.info(f"Truncating data for {key}...")
            extra_vars = self._get_extra_vars("truncate", key)
            conn_type = self.conn_type_dict["truncate"][key]
            conn_dict = self.conn_info_dict["truncate"][key]
            raw_stmt = self.configs_dict["truncate_sql_stmts_dict"][key]
            stmt = _render_stmt(raw_stmt, extra_vars)
            logger.info(f"     Truncate query: {stmt}")
            try:
                sql_exec_stmt(
                    stmt,
                    conn_dict,
                    mode=kwargs.get("mode", conn_type),
                    **{k: v for k, v in kwargs.items() if k != "mode"},
                )
            except Exception as e:
                logger.error(f"Error truncating data: {type(e).__name__} - {e}")
                raise

    def read_data(self, **kwargs):
        """Read data from each configured ``download`` connection.

        Results are stored in ``self.raw_data`` keyed by the configuration key.
        """
        self._require_process("download")
        self.raw_data: dict = {}
        for key in self.configs_dict["download_connections_dict"].keys():
            logger.info(f"Downloading data for {key}...")
            extra_vars = self._get_extra_vars("download", key)
            conn_type = self.conn_type_dict["download"][key]
            conn_dict = self.conn_info_dict["download"][key]
            raw_stmt = self.configs_dict["download_sql_stmts_dict"][key]
            stmt = _render_stmt(raw_stmt, extra_vars)
            logger.info(f"     Download query: {stmt}")

            ## Resolve per-call kwargs with sensible config-based defaults
            custom_conn_str = self._kwargs_or_config(
                "download", key, "custom_conn_str", None, kwargs
            )
            connect_args = self._kwargs_or_config(
                "download", key, "connect_arg", {}, kwargs
            )
            name = (
                kwargs.get("name")
                or (self.configs_dict.get("download_tables_dict") or {}).get(key, key)
            )
            max_n_try = kwargs.get(
                "max_n_try", self.configs_dict.get("max_n_try", 3)
            )
            log_file_path = kwargs.get(
                "log_file_path", self.configs_dict.get("log_file_path", "logs")
            )

            data = sql_read_data(
                stmt,
                conn_dict,
                custom_conn_str=custom_conn_str,
                mode=kwargs.get("mode", conn_type),
                connect_args=connect_args,
                name=name,
                max_n_try=max_n_try,
                log_file_path=log_file_path,
                **{
                    k: v
                    for k, v in kwargs.items()
                    if k not in (
                        "custom_conn_str",
                        "mode",
                        "connect_args",
                        "name",
                        "max_n_try",
                        "log_file_path",
                    )
                },
            )
            self.raw_data[key] = data.copy()

    def upload_data(self, data_to_upload: dict, **kwargs):
        """Upload data to each configured ``upload`` connection.

        Parameters:
            data_to_upload (dict): Mapping ``key -> pandas.DataFrame``.
        """
        self._require_process("upload")
        for key in self.configs_dict["upload_connections_dict"].keys():
            logger.info(f"Uploading data for {key}...")
            upload_df = data_to_upload[key]
            conn_type = self.conn_type_dict["upload"][key]
            conn_dict = self.conn_info_dict["upload"][key]
            logger.info(
                f"     {conn_type.capitalize()} table: "
                f"{self.configs_dict['upload_tables_dict'][key]}"
            )

            ## Build SQLAlchemy dtype dictionary safely
            col_dict = self.configs_dict["upload_python_to_sql_dtypes_dict"][key]
            dtypes_dict = self._build_dtypes_dict(key)
            ## Enforce column ordering defined by the dtype dictionary
            upload_df = upload_df[list(col_dict.keys())]

            ## Resolve per-call kwargs with sensible config-based defaults
            custom_conn_str = self._kwargs_or_config(
                "upload", key, "custom_conn_str", None, kwargs
            )
            connect_args = self._kwargs_or_config(
                "upload", key, "connect_arg", {}, kwargs
            )
            chunksize = self._kwargs_or_config(
                "upload", key, "chunksize", 100, kwargs
            )
            method = self._kwargs_or_config(
                "upload", key, "method", "multi", kwargs
            )
            name = kwargs.get("name", self.configs_dict["upload_tables_dict"][key])
            max_n_try = kwargs.get(
                "max_n_try", self.configs_dict.get("max_n_try", 3)
            )
            n_jobs = kwargs.get("n_jobs", self.configs_dict.get("n_parallel", -1))
            log_file_path = kwargs.get(
                "log_file_path", self.configs_dict.get("log_file_path", "logs")
            )

            sql_upload_data(
                upload_df,
                self.configs_dict["upload_schemas_dict"][key],
                self.configs_dict["upload_tables_dict"][key],
                conn_dict,
                custom_conn_str=custom_conn_str,
                mode=kwargs.get("mode", conn_type),
                connect_args=connect_args,
                name=name,
                chunksize=chunksize,
                method=method,
                dtypes_dict=kwargs.get("dtypes_dict", dtypes_dict),
                max_n_try=max_n_try,
                n_jobs=n_jobs,
                log_file_path=log_file_path,
                **{
                    k: v
                    for k, v in kwargs.items()
                    if k not in (
                        "custom_conn_str",
                        "mode",
                        "connect_args",
                        "name",
                        "chunksize",
                        "method",
                        "dtypes_dict",
                        "max_n_try",
                        "n_jobs",
                        "log_file_path",
                    )
                },
            )
