#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module is preparing the python path to access all modules.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

# ToDo: PATHs im Program auf _paths.py umstellen.
all_paths = {
    'yaml_path': "",
    'program_config_path': "",
    'template_path': "",
}

package_path = os.path.dirname(os.path.realpath(__file__))
