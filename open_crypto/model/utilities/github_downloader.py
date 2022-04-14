#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module is essentially taken from 'https://github.com/sdushantha/gitdir'. Full credit to the author and many thanks!
Smaller adjustments are made, in particular regarding the print statements. The functions are refactored into methods
or moved to different classes.
"""

import json
import os
import re
import signal
import sys
import urllib.request

from _paths import all_paths
from model.utilities.loading_bar import Loader


class GitDownloader:
    """
    Class to download, in this case update, files directly from the Github repository. This is needed to react on
    frequently changing exchange API mappings without the need to create a new PyPI version. The class is called
    in the runner module, in particular with: runner.update_maps().
    """

    @staticmethod
    def create_url(url: str) -> str:
        """
        From the given url, produce a URL that is compatible with Github's REST API. Can handle blob or tree paths.
        @param url: The repository url.
        @return api_url, download_dirs
        """
        repo_only_url = re.compile(r"https://github\.com/[a-z\d](?:[a-z\d]|-(?=[a-z\d])){0,38}/[a-zA-Z0-9]+$")
        re_branch = re.compile("/(tree|blob)/(.+?)/")

        # Check if the given url is a url to a GitHub repo. If it is, tell the
        # user to use 'git clone' to download it
        if re.match(repo_only_url, url):
            print("✘ The given url is a complete repository. Use 'git clone' to download the repository")
            sys.exit()

        # extract the branch name from the given url (e.g master)
        branch = re_branch.search(url)
        download_dirs = url[branch.end():]
        api_url = (url[:branch.start()].replace("github.com", "api.github.com/repos", 1) +
                   "/contents/" + download_dirs + "?ref=" + branch.group(2))
        return api_url

    @staticmethod
    def download(repo_url: str, output_dir: str = "./resources/running_exchanges/") -> None:
        """
        Downloads the files and directories

        @param repo_url: The repository-url.
        @param output_dir: The output directory
        """
        # generate the url which returns the JSON data
        api_url = GitDownloader.create_url(repo_url)

        opener = urllib.request.build_opener()
        opener.addheaders = [("User-agent", "Mozilla/5.0")]
        urllib.request.install_opener(opener)
        response = urllib.request.urlretrieve(api_url)

        with open(response[0], "r", encoding="UTF-8") as resp:
            data = json.load(resp)

            # If the data is a file, download it as one.
            if isinstance(data, dict) and data["type"] == "file":
                # download the file
                opener = urllib.request.build_opener()
                opener.addheaders = [("User-agent", "Mozilla/5.0")]
                urllib.request.install_opener(opener)
                urllib.request.urlretrieve(data["download_url"], os.path.join(output_dir, data["name"]))
                # bring the cursor to the beginning, erase the current line, and dont make a new line

            loader: Loader
            with Loader("Updating exchange mappings from GitHub..", "✔ Exchange mapping update complete",
                        max_counter=len(data)) as loader:
                for file in data:
                    file_url = file["download_url"]

                    if file_url is not None:
                        opener = urllib.request.build_opener()
                        opener.addheaders = [("User-agent", "Mozilla/5.0")]
                        urllib.request.install_opener(opener)
                        # download the file
                        urllib.request.urlretrieve(file_url, output_dir + file["name"])

                    else:
                        GitDownloader.download(file["html_url"], output_dir)

                    loader.increment()

    @staticmethod
    def main() -> None:
        """
        Run the downloader.
        """
        if sys.platform != "win32":
            # disable CTRL+Z
            signal.signal(signal.SIGTSTP, signal.SIG_IGN)

        url = "https://github.com/SteffenGue/open-crypto/tree/master/open_crypto/resources/running_exchanges"

        resource_path = all_paths.get("package_path").__str__() + "/resources/running_exchanges/"

        GitDownloader.download(url, output_dir=resource_path)
