# Import modules
import datetime as dt
import functools
import logging
import os
import subprocess
import sys

# Import submodules
from concurrent.futures import ProcessPoolExecutor

# Import third-party modules
from colorama import Fore


# Module-level logger (inherits handlers configured by the host application)
logger = logging.getLogger(__name__)


# ============================================================================
# Helper functions
# ============================================================================


def _emit_log(
    file_path: str,
    file_name_wext: str,
    header_lines: list[str],
    body_lines: list[str],
    save_logs: bool = False,
    show_output: bool = False,
) -> None:
    """Helper: write header + body lines to a log file and optionally log them.

    - Creates the file with header when missing.
    - Appends body lines when ``save_logs`` is True.
    - Emits header+body through the logger when ``show_output`` is True.
    """
    # Write to file
    if save_logs:
        ## Create log file with header if missing
        if file_name_wext not in os.listdir(file_path):
            with open(f"{file_path}/{file_name_wext}", "w") as l_f:
                for ln in header_lines:
                    l_f.write(ln)
        ## Append body
        with open(f"{file_path}/{file_name_wext}", "a") as l_f:
            for ln in body_lines:
                l_f.write(ln)
    # Log to console (no longer using print)
    if show_output:
        logger.info("\n".join(header_lines + body_lines))


# ============================================================================
# Main functions
# ============================================================================


def parallel_execute(applyFunc, *args, **kwargs):
    """
    Execute a function in parallel using a ``ProcessPoolExecutor``.

    Parameters:
        applyFunc: Callable to apply in parallel.
        *args: Iterables. One iterable per positional argument of ``applyFunc``.
        **kwargs: Keyword arguments bound to ``applyFunc`` via ``functools.partial``
                  before parallel execution.

    Returns:
        list: Results of each parallel invocation, in input order.
    """
    # Bind keyword arguments
    if kwargs:
        func = functools.partial(applyFunc, **kwargs)
    else:
        func = applyFunc

    # Run in parallel and materialise results
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(func, *args))

    return results


def mk_exec_logs(
    file_path: str,
    file_name: str,
    process_name: str,
    output_content: str,
    show_output: bool = False,
    save_logs: bool = False,
) -> None:
    """
    Create/save execution log files.

    Parameters:
        file_path (str): File path to use for saving logs.
        file_name (str): File name to use for log file (without extension).
        process_name (str): Process name.
        output_content (str): Output content.
        show_output (bool): Show output content via the logger.
        save_logs (bool): Save logs to file.

    Returns:
        None
    """
    # Validate parameters
    assert (
        isinstance(file_path, str) and file_path.strip()
    ), "file_path must be a non-empty string"
    assert (
        isinstance(file_name, str) and file_name.strip()
    ), "file_name must be a non-empty string"
    assert (
        isinstance(process_name, str) and process_name.strip()
    ), "process_name must be a non-empty string"

    # Set file name
    file_name_wext = f"{file_name}.log"

    # Generate log content
    title_str = "#                    Process output                    #"
    header_lines = [
        f'\n\n{"#" * len(title_str)}',
        title_str,
        f'{"#" * len(title_str)}\n\n',
    ]
    body_lines = [
        f"Date:\n\n {dt.datetime.strftime(dt.datetime.now(), '%Y/%m/%d %H:%M:%S')}\n\n",
        f"Process name:\n\n {process_name}\n\n",
        f"Output:\n\n {output_content}\n\n",
        "------------------------------------------------------\n\n",
    ]

    # Emit log
    _emit_log(
        file_path, file_name_wext, header_lines, body_lines, save_logs, show_output
    )


def mk_texec_logs(
    file_path: str,
    file_name: str,
    time_var: str,
    time_val,
    obs: str | None = None,
    show_output: bool = False,
    save_logs: bool = False,
) -> None:
    """
    Create/save time-execution log files.

    Parameters:
        file_path (str): File path to use for saving logs.
        file_name (str): File name to use for log file (without extension).
        time_var (str): Time variable's name.
        time_val: Time variable's value (typically a ``timedelta``).
        obs (str | None): Observations.
        show_output (bool): Show output content via the logger.
        save_logs (bool): Save logs to file.

    Returns:
        None
    """
    # Validate parameters
    assert (
        isinstance(file_path, str) and file_path.strip()
    ), "file_path must be a non-empty string"
    assert (
        isinstance(file_name, str) and file_name.strip()
    ), "file_name must be a non-empty string"
    assert (
        isinstance(time_var, str) and time_var.strip()
    ), "time_var must be a non-empty string"

    # Set file name
    file_name_wext = f"{file_name}.log"

    # Generate log content
    title_str = "# Time variable          Time value          Date          Observations          #"
    header_lines = [
        f'\n\n{"#" * len(title_str)}',
        title_str,
        f'{"#" * len(title_str)}\n\n',
    ]
    body_lines = [
        f"{time_var}          {time_val}          "
        f"{dt.datetime.strftime(dt.datetime.now(), '%Y/%m/%d %H:%M:%S')}          {obs}"
    ]

    # Emit log
    _emit_log(
        file_path, file_name_wext, header_lines, body_lines, save_logs, show_output
    )


def mk_err_logs(
    file_path: str,
    file_name: str,
    err_var: str,
    err_desc: str,
    mode: str = "summary",
    show_output: bool = False,
    save_logs: bool = False,
) -> None:
    """
    Create/save error log files.

    Parameters:
        file_path (str): File path to use for saving logs.
        file_name (str): File name to use for log file (without extension).
        err_var (str): Error variable name.
        err_desc (str): Error description.
        mode (str): Mode to use for log file (``"summary"`` or ``"detailed"``).
        show_output (bool): Show output content via the logger.
        save_logs (bool): Save logs to file.

    Returns:
        None
    """
    # Validate parameters
    assert (
        isinstance(file_path, str) and file_path.strip()
    ), "file_path must be a non-empty string"
    assert (
        isinstance(file_name, str) and file_name.strip()
    ), "file_name must be a non-empty string"
    assert (
        isinstance(err_var, str) and err_var.strip()
    ), "err_var must be a non-empty string"
    assert (
        isinstance(err_desc, str) and err_desc.strip()
    ), "err_desc must be a non-empty string"

    # Set file name
    file_name_wmode_wext = f"{file_name}_{mode.lower()}.log"

    # Generate log content
    if mode.lower() == "summary":
        title_str = (
            "# Error variable          Error description          Date          #"
        )
        header_lines = [
            f'\n\n{"#" * len(title_str)}',
            title_str,
            f'{"#" * len(title_str)}\n\n',
        ]
        body_lines = [
            f"{err_var}          {err_desc}          "
            f"{dt.datetime.strftime(dt.datetime.now(), '%Y/%m/%d %H:%M:%S')}"
        ]
    elif mode.lower() == "detailed":
        title_str = (
            "#                    Detailed error description                    #"
        )
        header_lines = [
            f'\n\n{"#" * len(title_str)}',
            title_str,
            f'{"#" * len(title_str)}\n\n',
        ]
        body_lines = [
            f"Date:\n\n {dt.datetime.strftime(dt.datetime.now(), '%Y/%m/%d %H:%M:%S')}\n\n",
            f"Error variable:\n\n {err_var}\n\n",
            f"Error description:\n\n {err_desc}\n\n",
            "------------------------------------------------------\n\n",
        ]
    else:
        raise ValueError(
            f"Invalid mode '{mode}'. Allowed values are: 'summary', 'detailed'."
        )

    # Emit log
    _emit_log(
        file_path,
        file_name_wmode_wext,
        header_lines,
        body_lines,
        save_logs,
        show_output,
    )


def execute_script(
    process_cmd_list,
    log_file_path: str = "logs",
    exec_log_file_name: str = "exec.log",
    texec_log_file_name: str = "txec.log",
    show_output: bool = False,
    save_logs: bool = False,
):
    """
    Execute a script in a subprocess and save execution logs.

    Parameters:
        process_cmd_list (list[str]): List-formatted process command to execute
                                      (e.g. ``["ls", "-la"]``). Passed with
                                      ``shell=False`` to avoid shell-injection.
        log_file_path (str): File path to use for saving logs.
        exec_log_file_name (str): Execution log file name.
        texec_log_file_name (str): Time execution log file name.
        show_output (bool): Forwarded to ``mk_exec_logs``/``mk_texec_logs``.
        save_logs (bool): Forwarded to ``mk_exec_logs``/``mk_texec_logs``.

    Returns:
        str: The captured stdout/stderr of the subprocess.
    """
    # Execute process
    s = dt.datetime.now()
    try:
        r = subprocess.check_output(
            process_cmd_list,
            shell=False,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        # Surface non-zero exit codes through the logger and propagate
        logger.error(
            f"{Fore.RED}Subprocess failed (returncode={e.returncode}): "
            f"{' '.join(process_cmd_list)}{Fore.RESET}\n{e.output}"
        )
        raise
    e_t = dt.datetime.now()
    logger.info(
        f"----- Process execution duration = {Fore.GREEN}{e_t - s}{Fore.RESET} -----"
    )

    # Create execution logs

    ## Set process string
    process_str = " ".join(process_cmd_list)
    ## Create logs directory
    os.makedirs(log_file_path, exist_ok=True)
    ## Create logs
    mk_exec_logs(
        log_file_path,
        exec_log_file_name,
        f"'{process_str}'",
        r,
        show_output=show_output,
        save_logs=save_logs,
    )
    mk_texec_logs(
        log_file_path,
        texec_log_file_name,
        f"'{process_str}'",
        e_t - s,
        show_output=show_output,
        save_logs=save_logs,
    )

    return r
