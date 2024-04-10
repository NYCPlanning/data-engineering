"""Written so that jupyter notebooks in /notebooks folder can be run as if they were run from root directory
Code to run in beginning of jupyter notebook:
import sys
sys.path.append('../utils')
import wd_management
wd_management.set_wd_root()
"""
import os


def correct_wd():
    return os.getcwd().split("/")[-1] == "db-equitable-development-tool"


def set_wd_root():
    print(f"called set_wd_root. current working directory is {os.getcwd()}")
    while not correct_wd():
        os.chdir("..")
