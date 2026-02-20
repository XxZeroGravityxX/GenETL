# Import modules
import datetime as dt
import os
import subprocess

# Import submodules
from concurrent.futures import ProcessPoolExecutor
from colorama import Fore


# Helper functions


def _emit_log(
    file_path: str,
    file_name_wext: str,
    header_lines: list[str],
    body_lines: list[str],
    save_logs: bool = False,
    show_output: bool = False,
) -> None:
    """Helper: write header + body lines to log file and optionally print.

    - Creates file with header when missing.
    - Appends body lines when `save_logs` is True.
    - Prints header+body when `show_output` is True.
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
    # Print to console
    if show_output:
        print("".join(header_lines + body_lines))

    pass


# Main functions


def mk_exec_logs(
    file_path: str,
    file_name: str,
    process_name: str,
    output_content: str,
    show_output: bool = False,
    save_logs: bool = False,
) -> None:
    """
    Function to create/save execution log files.


    Parameters:
        file_path (str): File path to use for saving logs.
        file_name (str): File name to use for log file.
        process_name (str): Process name.
        output_content (str): Output content.
        show_output (bool): Show output content in console.
        save_logs (bool): Save logs to file.

        Returns : None
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
        "#" * len(title_str) + "\n",
        title_str + "\n",
        "#" * len(title_str) + "\n\n",
    ]
    body_lines = [
        f"Date:\n\n {dt.datetime.strftime(dt.datetime.now(),'%Y/%m/%d %H:%M:%S')}\n\n",
        f"Process name:\n\n {process_name}\n\n",
        f"Output:\n\n {output_content}\n\n",
        "------------------------------------------------------\n\n",
    ]

    # Emit log
    _emit_log(
        file_path, file_name_wext, header_lines, body_lines, save_logs, show_output
    )

    pass


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
    Function to create/save log time execution files.

    Parameters:
        file_path (str): File path to use for saving logs.
        file_name (str): File name to use for log file.
        time_var (str): Time variable's name.
        time_val (str): Time variable's value.
        obs (str): Observations.
        show_output (bool): Show output content in console.

    Returns: None
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
        "#" * len(title_str) + "\n",
        title_str + "\n",
        "#" * len(title_str) + "\n\n",
    ]
    body_lines = [
        f"{time_var}          {time_val}          {dt.datetime.strftime(dt.datetime.now(),'%Y/%m/%d %H:%M:%S')}          {obs}"
        + "\n"
    ]

    # Emit log
    _emit_log(
        file_path, file_name_wext, header_lines, body_lines, save_logs, show_output
    )

    pass


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
    Function to create/save log error files.

    Parameters:
        file_path (str): File path to use for saving logs.
        file_name (str): File name to use for log file.
        err_var (str): Error variable name.
        err_desc (str): Error description.
        mode (str): Mode to use for log file.
        show_output (bool): Show output content in console.

    Returns: None
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
            "#" * len(title_str) + "\n",
            title_str + "\n",
            "#" * len(title_str) + "\n\n",
        ]
        body_lines = [
            f"{err_var}          {err_desc}          {dt.datetime.strftime(dt.datetime.now(),'%Y/%m/%d %H:%M:%S')}"
            + "\n"
        ]
    elif mode.lower() == "detailed":
        title_str = (
            "#                    Detailed error description                    #"
        )
        header_lines = [
            "#" * len(title_str) + "\n",
            title_str + "\n",
            "#" * len(title_str) + "\n\n",
        ]
        body_lines = [
            f"Date:\n\n {dt.datetime.strftime(dt.datetime.now(),'%Y/%m/%d %H:%M:%S')}\n\n",
            f"Error variable:\n\n {err_var}\n\n",
            f"Error description:\n\n {err_desc}\n\n",
            "------------------------------------------------------\n\n",
        ]
    # Emit log
    _emit_log(
        file_path,
        file_name_wmode_wext,
        header_lines,
        body_lines,
        save_logs,
        show_output,
    )

    pass


def parallel_execute(applyFunc, *args, **kwargs):
    """
    Function to execute function parallely.

    Parameters:
        applyFunc: Function. Function to apply parallely.
        args: Iterable. Arguments to pass to function on each parallel execution.
    """

    with ProcessPoolExecutor() as executor:
        results = executor.map(applyFunc, *args, **kwargs)

    return results


def execute_script(
    process_str,
    log_file_path="logs",
    exec_log_file_name="exec.log",
    texec_log_file_name="txec.log",
    show_output=False,
    save_logs=False,
):
    """
    Function to execute an script, saving execution logs.

    Parameters:
        process_str: String. Process to execute.
        log_file_path: String. File path to use for saving logs.
        exec_log_file_name: String. Execution log file name.
        texec_log_file_name: String. Time execution log file name.
    """

    # Execute process
    s = dt.datetime.now()
    r = subprocess.check_output(
        process_str,
        shell=True,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )
    e = dt.datetime.now()
    print(f"----- Process execution duration = {Fore.GREEN}{e-s}{Fore.RESET} -----")
    # Create execution logs
    os.makedirs(log_file_path, exist_ok=True)
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
        e - s,
        show_output=show_output,
        save_logs=save_logs,
    )

    pass
