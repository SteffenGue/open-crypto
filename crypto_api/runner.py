import os
import shutil

import main

path = os.getcwd()


def check_path():
    """
    Checks if all resources are in the current working directory. If not, calls the function update_maps()
    """
    destination = path + "/resources"
    if not os.path.exists(destination):
        update_maps()


def update_maps(cwd: str = path):
    """
    Copies everything from the folder "resources" into the current working directory. If files already exist,
    the method will override them (i.e. first delete and then copy).
    @type cwd: Current working directory
    """

    print(f"Copying resources to {cwd}..")
    source = os.path.dirname(os.path.realpath(__file__)) + "/resources"

    destination = cwd + "/resources"
    for src_dir, dirs, files in os.walk(source):
        dst_dir = src_dir.replace(source, destination, 1)
        try:
            dirs.remove('templates')
            dirs.remove('__pycache__')
        except ValueError:
            pass

        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)
        for file_ in files:
            src_file = os.path.join(src_dir, file_)
            dst_file = os.path.join(dst_dir, file_)
            if os.path.exists(dst_file):
                # in case of the src and dst are the same file
                if os.path.samefile(src_file, dst_file):
                    continue
                os.remove(dst_file)
            if not src_file.endswith('.py'):
                shutil.copy(src_file, dst_dir)


def run(cwd=path):
    """
    Firstly checks if all necessary folder are available (i.e. config and yaml-maps) and runs the program.
    @param cwd: The current working directory if not specified differently.
    """
    check_path()
    run(main.run(cwd))


if __name__ == '__main__':
    run(path)
