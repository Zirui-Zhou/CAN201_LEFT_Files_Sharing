# -*- coding: UTF-8 -*-
"""
# @Author:  Zirui Zhou
# @Date:    2021/10/24 00:16:41
# @Contact: zirui.zhou19@student.xjtlu.edu.cn
"""

import math
import os


TEMP_FILE_SIZE = 32 * (1024 * 1024)
DATA_SIZE = 32 * 1024


class fileLoader(object):
    """An iterator-like class to split big files into blocks.

    Attributes:
        filename: A string of the relative path of the file.
        file_size: A integer of the size of the file.
        block_num: A integer of the number of split blocks by TEMP_FILE_SIZE.
        it: A iterator of number of blocks.
    """
    def __init__(self, filename):
        self.filename = filename
        self.file_size = os.path.getsize(self.filename)
        self.block_num = math.ceil(self.file_size / TEMP_FILE_SIZE)
        self.it = iter(range(self.block_num))

    def __iter__(self):
        return self

    def __next__(self):
        return self.filename, next(self.it)


class blockLoader(object):
    """An iterator-like class to read a split block.

    Attributes:
        filename: A string of the relative path of the target file.
        block_index: A integer of the index of this block.
        _start: A protected integer of start position for read().
        _end: A protected integer of end position for read().
        fp: A file pointer of the target file.
    """
    def __init__(self, filename, block_index=0):
        self.filename = filename
        self.block_index = block_index
        self._start = self.block_index * TEMP_FILE_SIZE
        self._end = min(self._start + TEMP_FILE_SIZE, self.file_size)

        self.fp = open(self.filename, 'rb')
        self.fp.seek(self._start, os.SEEK_SET)

    def __iter__(self):
        return self

    def __next__(self):
        """Iterate next part of data in the block.

        Returns:
            A integer of the position of the data.
            A bytes of file data of DATA_SIZE in maximum.
        """
        # If the pointer is at the end of the block, stop reading.
        if self.fp.tell() == self._end:
            self.fp.close()
            raise StopIteration

        position = self.fp.tell()
        data = self.fp.read(min(self._end - position, DATA_SIZE))

        return position, data

    @property
    def file_size(self):
        return os.path.getsize(self.filename)


class fileWriter(object):
    """A file-like class to write a file from unpacked split data.

    Attributes:
        filename: A string of the relative path of the target file.
        file_size: A integer of the file size of the file.
        fp: A file pointer of the target file.
    """
    def __init__(self, filename, file_size):
        self.filename = filename
        self.file_size = file_size

        # If the path is not existed, create the path.
        filepath = os.path.dirname(filename)
        os.makedirs(filepath, exist_ok=True)

        # If the file is not existed, create the file.
        open(filename, 'ab').close()
        self.fp = open(filename, 'rb+')

    def write(self, position, data):
        self.fp.seek(position, os.SEEK_SET)
        self.fp.write(data)

    def close(self):
        self.fp.truncate(self.file_size)
        self.fp.close()


def get_file_info(filename):
    """Calculate the md5 of the target file.

    This function depends on the md5sum shell-command in Linux.

    Args:
        filename: A string of the relative path of the target file.

    Returns:
        A string of the md5 of the target file.
    """
    # TODO(Zirui): Make md5 calculation compatible in Windows.
    md5 = os.popen('md5sum ' + filename).read().split()[0]
    return md5
