"""Hook to extract python code from qmd files and write to a new file with the
same name, but in python. Then run flake8 on the new file. This hook is
executed before a commit is made."""
import os
import re


FLAKE_8_NOT_FOUND_CODE = 32512

# Get the current working directory
cwd = os.getcwd()

# Get the notebooks folder
notebooks_folder = os.path.join(cwd, "notebooks")

# Get the qmd files
qmd_files = [f for f in os.listdir(notebooks_folder) if f.endswith(".qmd")]

# Get the python code from the qmd files and write to a new file with the same
# name
for qmd_file in qmd_files:

    file_path = os.path.join(notebooks_folder, qmd_file)
    with open(file_path, "r", encoding="utf-8") as file:
        content = file.read()
    # Get the python code
    python_code = re.findall(r"```{python}(.*?)```", content, re.DOTALL)
    python_code = "\n".join(python_code)
    # Write the python code to a new file
    new_file_path = file_path.replace(".qmd", ".py")
    with open(new_file_path, "w", encoding="utf-8") as file:
        file.write(python_code)
    # The ignored rules are:
    # E402: module level import not at top of file
    # E129: visually indented line with same indent as next logical line
    # W504: line break after binary operator
    response = os.system(f"flake8 {new_file_path} --ignore=E402,E129,W504")
    if response == FLAKE_8_NOT_FOUND_CODE:
        print("Flake8 not installed")
        os.system("pip install flake8")
        response = os.system(f"flake8 {new_file_path} --ignore=E402,E129,W504")

    print(response)
    if response != 0:
        raise ValueError("Flake8 failed")

    os.remove(new_file_path)
