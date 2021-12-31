# -*- coding: UTF-8 -*-
"""
# @Author:  Zirui Zhou
# @Date:    2021/10/20 21:20:12
# @Contact: zirui.zhou19@student.xjtlu.edu.cn
"""

import argparse

import fileSocket


def _argparse():
    parser = argparse.ArgumentParser(description="The target ipv4 address.")
    parser.add_argument('--ip', action='store', required=True, dest='ip', help='ip')

    return parser.parse_args()


def main():
    parser = _argparse()
    # The port should be between 20000 and 30000.
    new_fileSocket = fileSocket.fileSocket(ip=parser.ip, port=25795, sock_num=4,
                                           share_folder='./share')
    new_fileSocket.start()


if __name__ == '__main__':
    main()
