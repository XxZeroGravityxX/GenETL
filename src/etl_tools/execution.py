# Import modules
import datetime as dt
import os
import subprocess

# Import submodules
from concurrent.futures import ProcessPoolExecutor
from colorama import Fore


def mk_exec_logs(file_path, file_name, process_name, output_content, show_output=False):
    """
    Function to create/save execution log files.

    Parameters:
        file_path: String. File path to use for saving logs.
        file_name: String. File name to use for log file.
        process_name: String. Process name.
        output_content: String. Output content.
        show_output: Boolean. Show output content in console.
    """

    # Set file name

    file_name_wext = f"{file_name}.log"

    # Generate log file

    ## Create log file if not in files
    if file_name_wext not in os.listdir(file_path):
        with open(f"{file_path}/{file_name_wext}", "w") as l_f:
            title_str = "#                    Process output                    #"
            l_f.write("#" * len(title_str) + "\n")
            l_f.write(title_str + "\n")
            l_f.write("#" * len(title_str) + "\n\n")
    ## Append log info
    with open(f"{file_path}/{file_name_wext}", "a") as l_f:
        l_f.write(
            f"Date:\n\n {dt.datetime.strftime(dt.datetime.now(),'%Y/%m/%d %H:%M:%S')}\n\n"
        )
        l_f.write(f"Process name:\n\n {process_name}\n\n")
        l_f.write(f"Output:\n\n {output_content}\n\n")
        l_f.write("------------------------------------------------------\n\n")

    # Show output content

    if show_output:
        ## Show log content in console
        title_str = "#                    Process output                    #"
        print("#" * len(title_str) + "\n")
        print(title_str + "\n")
        print("#" * len(title_str) + "\n\n")
        print(
            f"Date:\n\n {dt.datetime.strftime(dt.datetime.now(),'%Y/%m/%d %H:%M:%S')}\n\n"
        )
        print(f"Process name:\n\n {process_name}\n\n")
        print(f"Output:\n\n {output_content}\n\n")

    pass


def mk_texec_logs(
    file_path, file_name, time_var, time_val, obs=None, show_output=False
):
    """
    Function to create/save log time execution files.

    Parameters:
        file_path: String. File path to use for saving logs.
        file_name: String. File name to use for log file.
        time_val: String. Time variable's value.
        time_var: String. Time variable's name.
        obs: String. Observations.
        show_output: Boolean. Show output content in console.
    """

    # Set file name

    file_name_wmode = f"{file_name}.log"

    # General log file

    ## Create log file if not in files
    if file_name_wmode not in os.listdir(file_path):
        with open(f"{file_path}/{file_name_wmode}", "w") as l_f:
            title_str = "# Time variable          Time value          Date          Observations          #"
            l_f.write("#" * len(title_str) + "\n")
            l_f.write(title_str + "\n")
            l_f.write("#" * len(title_str) + "\n\n")
    ## Append log info
    with open(f"{file_path}/{file_name_wmode}", "a") as l_f:
        l_f.write(
            f"{time_var}          {time_val}          {dt.datetime.strftime(dt.datetime.now(),'%Y/%m/%d %H:%M:%S')}          {obs}"
            + "\n"
        )

    # Show output content

    if show_output:
        ## Show log content in console
        title_str = "# Time variable          Time value          Date          Observations          #"
        print("#" * len(title_str) + "\n")
        print(title_str + "\n")
        print("#" * len(title_str) + "\n\n")
        print(
            f"{time_var}          {time_val}          {dt.datetime.strftime(dt.datetime.now(),'%Y/%m/%d %H:%M:%S')}          {obs}"
            + "\n"
        )

    pass


def mk_err_logs(
    file_path, file_name, err_var, err_desc, mode="summary", show_output=False
):
    """
    Function to create/save log error files.

    Parameters:
        file_path: String. File path to use for saving logs.
        file_name: String. File name to use for log file.
        err_desc: String. Error description.
        err_var: String. Error variable name.
        mode: String. Mode to use for log file.
        show_output: Boolean. Show output content in console.
    """

    # Set file name

    file_name_wmode = f"{file_name}_{mode.lower()}.log"

    # Generate log file

    if mode.lower() == "summary":
        ## General log file

        ### Create log file if not in files
        if file_name_wmode not in os.listdir(file_path):
            with open(f"{file_path}/{file_name_wmode}", "w") as l_f:
                title_str = "# Error variable          Error description          Date          #"
                l_f.write("#" * len(title_str) + "\n")
                l_f.write(title_str + "\n")
                l_f.write("#" * len(title_str) + "\n\n")
        ### Append log info
        with open(f"{file_path}/{file_name_wmode}", "a") as l_f:
            l_f.write(
                f"{err_var}          {err_desc}          {dt.datetime.strftime(dt.datetime.now(),'%Y/%m/%d %H:%M:%S')}"
                + "\n"
            )

        ## Show output content

        if show_output:
            ## Show log content in console
            title_str = (
                "# Error variable          Error description          Date          #"
            )
            print("#" * len(title_str) + "\n")
            print(title_str + "\n")
            print("#" * len(title_str) + "\n\n")
            print(
                f"{err_var}          {err_desc}          {dt.datetime.strftime(dt.datetime.now(),'%Y/%m/%d %H:%M:%S')}"
                + "\n"
            )

    elif mode.lower() == "detailed":
        ## Detailed log file

        ### Create log file if not in files
        if file_name_wmode not in os.listdir(file_path):
            with open(f"{file_path}/{file_name_wmode}", "w") as l_f:
                title_str = "#                    Detailed error description                    #"
                l_f.write("#" * len(title_str) + "\n")
                l_f.write(title_str + "\n")
                l_f.write("#" * len(title_str) + "\n\n")
        ### Append log info
        with open(f"{file_path}/{file_name_wmode}", "a") as l_f:
            l_f.write(
                f"Date:\n\n {dt.datetime.strftime(dt.datetime.now(),'%Y/%m/%d %H:%M:%S')}\n\n"
            )
            l_f.write(f"Error variable:\n\n {err_var}\n\n")
            l_f.write(f"Error description:\n\n {err_desc}\n\n")
            l_f.write("------------------------------------------------------\n\n")

        ## Show output content

        if show_output:
            ## Show log content in console
            title_str = (
                "#                    Detailed error description                    #"
            )
            print("#" * len(title_str) + "\n")
            print(title_str + "\n")
            print("#" * len(title_str) + "\n\n")
            print(
                f"Date:\n\n {dt.datetime.strftime(dt.datetime.now(),'%Y/%m/%d %H:%M:%S')}\n\n"
            )
            print(f"Error variable:\n\n {err_var}\n\n")
            print(f"Error description:\n\n {err_desc}\n\n")

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
    )
    mk_texec_logs(log_file_path, texec_log_file_name, f"'{process_str}'", e - s)

    pass
