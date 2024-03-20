# Import modules
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(".env")
# Change version of package
with open("pyproject.toml", "r+") as toml_file:
    # Read file
    data = toml_file.readlines()
    for i in range(len(data)):
        if data[i].startswith("version"):
            # Get next version
            current_version = data[i].split("=")[1].strip().replace('"', "").split(".")
            next_version = (
                f"{current_version[0]}.{current_version[1]}.{int(current_version[2])+1}"
            )
            # Update version
            data[i] = f'version = "{next_version}"\n'
    # Write changes to file
    toml_file.seek(0)
    toml_file.writelines(data)
# Execute commands
os.system("git add pyproject.toml")  # Add changes to git
os.system('git commit -m "New version build."')  # Commit changes
os.system("git push")  # Push changes
os.system("rm -rf dist/*")  # Remove old distribution files
os.system("python -m build")  # Build package
os.system(
    f'twine upload --repository testpypi dist/* -p {os.getenv("TESTPYPI_API_TOKEN")}'
)  # Upload to test pypi
os.system(
    f'twine upload --repository pypi dist/* -p {os.getenv("PYPI_API_TOKEN")}'
)  # Upload to pypi
