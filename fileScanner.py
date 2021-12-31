# -*- coding: UTF-8 -*-
"""
# @Author:  Zirui Zhou
# @Date:    2021/10/23 21:09:07
# @Contact: zirui.zhou19@student.xjtlu.edu.cn
"""

import os
import time

import fileLoader
import fileSocket


class fileScanner(object):
    """A class to scan changed files from a sync folder which can work in an independent process.

    Attributes:
        listen_path: A string of the listening sync path.
        interval_time: A integer of the interval time (second) between scanning.
        main_queue: A queue to transmit message from fileScanner.fileScanner to fileSocket.fileSocket.
        recv_dict: A inter-process dictionary of relative filepath (keys) and md5 (values) to
        determine whether the files are from other devices.
    """
    interval_time = 1

    def __init__(self, path, main_queue, recv_dict):
        self.listen_path = path
        self.main_queue = main_queue
        self.recv_dict = recv_dict

    @classmethod
    def load_file(cls, root, recv_dict):
        """Load all files' information in a root path.

        Args:
            root: A string of the root path to load files.
            recv_dict: A whitelist-like dictionary of relative filepath (keys) and block number
            (e.g. md5) (values) to filter files.

        Returns:
            A dictionary of all files' relative filepath (keys) and file_info (e.g. md5) (values).
        """
        file_dict = dict()
        for path, dirs, files, in os.walk(root, topdown=False):
            for name in files:
                filepath = os.path.join(path, name)
                file_info = cls.filter_file(filepath, recv_dict)
                # If the returned file_info is None, this file can be simply ignored.
                if file_info is not None:
                    file_dict[filepath] = file_info
        return file_dict

    @staticmethod
    def filter_file(filepath, recv_dict):
        """Filter the files in the recv_dict by given rules.

        Args:
            filepath: A string of the relative path of the file.
            recv_dict: A whitelist-like dictionary of relative filepath (keys) and block number
            (values) to filter files.

        Returns:
            None or specific file identification mark (e.g. md5).
        """
        file_info = fileLoader.get_file_info(filepath)

        # If the filepath in the dict, the filepath can be seen as protected in the whitelist.
        if filepath in recv_dict:
            # If the file_info is an integer, the transmission is not finished.
            if isinstance(recv_dict[filepath], int):
                return None
            # If the file_info is not changed, the file should be neglected to prevent add detection.
            if recv_dict[filepath] == file_info:
                return None
            # If the file_info is changed, the file should be removed from the whitelist.
            recv_dict.pop(filepath)

        return file_info

    @staticmethod
    def compare_file(old_file, new_file):
        """Compare the files in two dictionary by their values.

        Args:
            old_file: A dictionary of old files' relative filepath (keys) and file_info (values).
            new_file: A dictionary of new files' relative filepath (keys) and file_info (values).

        Returns:
            Four set of filepath for added, removed, same and updated files.
        """
        remove = old_file.keys() - new_file.keys()
        add = new_file.keys() - old_file.keys()
        same = old_file.keys() & new_file.keys()
        update = set([i for i in same if old_file[i] != new_file[i]])
        same -= update
        return add, remove, same, update

    @staticmethod
    def send_file(result, queue):
        """Send the filepath of the needed files into the main_queue to activate
        fileSocket.fileSocket.send_file().

        Args:
            result: A set of the return of compare_file().
            queue: A queue to transmit message to fileSocket.fileSocket.
        """
        for filename in result[0] | result[3]:  # Files in add and update.
            fileSocket.command.put(fileSocket.fileSocket.send_file, queue, filename)

    def main_loop(self):
        """Main loop to scan files.
        """
        old_file = self.load_file(self.listen_path, self.recv_dict)
        while True:
            time.sleep(self.interval_time)
            new_file = self.load_file(self.listen_path, self.recv_dict)
            result = self.compare_file(old_file, new_file)
            old_file = new_file.copy()
            self.send_file(result, self.main_queue)

    def start(self):
        """Start function for multiprocess.
        """
        self.main_loop()
