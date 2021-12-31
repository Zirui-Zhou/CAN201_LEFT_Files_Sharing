# -*- coding: UTF-8 -*-
"""
# @Author:  Zirui Zhou
# @Date:    2021/10/24 14:35:37
# @Contact: zirui.zhou19@student.xjtlu.edu.cn
"""

import multiprocessing
import socket
import struct
import threading

import fileLoader
import fileScanner


class _sendThread(threading.Thread):
    """A thread to send messages to dynamic sockets.

    Attributes:
        sock: A socket to send message to a target address which can reconnect.
        cmd_queue: A queue to read message from fileSocket.sendSocket.
    """
    def __init__(self, cmd_queue):
        threading.Thread.__init__(self)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.cmd_queue = cmd_queue
        self.daemon = True

    def run(self):
        """Start function for multithreading.
        """
        while True:
            self.get_command()

    def get_command(self):
        """Get command from cmd_queue.
        """
        command.get(self, self.cmd_queue, retry=socket.error)

    def connect(self, send_addr):
        """Connect to a target socket by infinite retrying.

        Args:
            send_addr: A address-like set of the target ip and port.
        """
        while True:
            try:
                self.sock.connect(send_addr)
                break
            except socket.error as e:
                pass

    def pause(self):
        """Pause the socket by closing connection.
        """
        self.sock.close()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def send_cont(self, send_addr, sock_num, is_echo):
        """Send CONT command to a target address.

        Args:
            send_addr: A address-like set of the target ip and port.
            sock_num: A integer of accepted number of sockets.
            is_echo: A bool of whether this command is an echo (send after receiving CONT).
        """
        self.connect(send_addr)
        self._send_cont(sock_num, is_echo)
        self.pause()

    def send_file(self, send_addr, block, block_num):
        """Send a block by continuous transmission to a target address.

        Args:
            send_addr: A address-like set of the target ip and port.
            block: A set of the arguments (filename, block_index) for fileLoader.blockLoader
            returned by fileLoader.fileLoader.
            block_num: A integer of the number of split blocks.
        """
        block = fileLoader.blockLoader(*block)
        filename = block.filename

        self.connect(send_addr)

        # Send a SEND command as the start mark of a block.
        self._send_send(filename, block_num, block.file_size)

        for position, data in block:
            self._send_pakg(position, data)

        # Send a VRFY command as the end mark of a block.
        self._send_vrfy()

        self.pause()

    def _send_package(self, code, *args):
        """Send a message by given code and arguments.

        Args:
            code: A _protocol.code of the sending package code.
            *args: The arguments of the message.
        """
        package = _protocol.pack(code, *args)
        # Send a guide package of a fixed length to tell the length of the message.
        self.sock.send(struct.pack(_protocol.guide_format, len(package)))
        self.sock.send(package)

    def _send_cont(self, sock_num, is_echo):
        self._send_package(_protocol.code.CONT, sock_num, is_echo)

    def _send_send(self, filename, block_num, file_size):
        self._send_package(_protocol.code.SEND, filename, block_num, file_size)

    def _send_pakg(self, position, data):
        self._send_package(_protocol.code.PAKG, position, data)

    def _send_vrfy(self):
        self._send_package(_protocol.code.VRFY)


class sendSocket(object):
    """A subprocess-based class to send messages.

    Attributes:
        thread_queue: A queue to transmit message from fileSocket.sendSocket to sub-threads.
        thread_list: A list of the _sendThread sub-threads.
        send_addr: A address-like set of the target ip and port.
        send_queue: A queue to transmit message to fileSocket.sendSocket.
    """
    thread_queue = multiprocessing.Queue()
    thread_list = list()

    def __init__(self, send_addr, send_queue):
        self.send_addr = send_addr
        self.send_queue = send_queue

    def start(self):
        """Start function for multiprocess.
        """
        while True:
            self.get_command()

    def get_command(self):
        """Get command from send_queue.
        """
        command.get(self, self.send_queue)

    def init_thread(self, num):
        """Initiate the socket threads for sending message.

        Args:
            num: A integer of the number of initiated threads.
        """
        for i in range(num):
            self.thread_list.append(_sendThread(self.thread_queue))
            self.thread_list[-1].start()

    def send_cont(self, sock_num, is_echo):
        """Send CONT command to the sending address.

        Args:
            sock_num: A integer of accepted number of sockets.
            is_echo: A bool of whether this command is an echo (send after receiving CONT).
        """
        command.put(_sendThread.send_cont, self.thread_queue, self.send_addr, sock_num, is_echo)

    def send_file(self, filename):
        """Send a file to the sending address.

        Args:
            filename: A string of the relative path of the file.
        """
        block_num = fileLoader.fileLoader(filename).block_num

        for block in fileLoader.fileLoader(filename):
            # Mark the start of sending a block.
            command.put(_sendThread.send_file, self.thread_queue, self.send_addr, block, block_num)


class _recvThread(threading.Thread):
    """A thread to receive messages from dynamic sockets.

    Attributes:
        recv_sock: A socket to bind the local address and accept connections.
        main_queue: A queue to transmit message from this thread to fileSocket.fileSocket.
    """
    def __init__(self, recv_sock, main_queue):
        threading.Thread.__init__(self)
        self.recv_sock = recv_sock
        self.main_queue = main_queue
        self.daemon = True

    def run(self):
        """Start function for multithreading. Keep listening if connection is established.
        """
        self.sock, addr = self.recv_sock.accept()
        while True:
            package = self._recv_package()
            # Discard empty package in some conditions.
            if package is None:
                continue
            self.recv_package(package)

    def pause(self):
        """Pause the socket by closing connection.
        """
        self.sock.close()
        self.sock, addr = self.recv_sock.accept()

    def _recv_package(self):
        """Receive a message by a guide package.

        Returns:
            None or A bytes of raw message.
        """
        # Receive the fixed-length guide package to decide the buffer size for message.
        # Apply socket.MSG_WAITALL to avoid receiving only half package.
        msg = self.sock.recv(struct.calcsize(_protocol.guide_format), socket.MSG_WAITALL)
        # Discard empty package in some conditions.
        if len(msg) == 0:
            return None
        buffer_size = struct.unpack(_protocol.guide_format, msg)[0]
        package = self.sock.recv(buffer_size, socket.MSG_WAITALL)
        return package

    def recv_package(self, package):
        """Parse a given message package.

        Args:
            package: A bytes of received message.

        Raises:
            Exception: A custom exception to receive unknown code.
        """
        package = _protocol.unpack(package)
        code = package[0]
        args = package[1:]
        if code == _protocol.code.CONT:
            self.recv_cont(*args)
        elif code == _protocol.code.SEND:
            self.recv_send(*args)
        elif code == _protocol.code.PAKG:
            self.recv_pakg(*args)
        elif code == _protocol.code.VRFY:
            self.recv_vrfy(*args)
        else:
            raise Exception("Recv Unknown Code: {}".format(code))

    def recv_cont(self, sock_num, is_echo):
        """Receive CONT command from another socket.

        Args:
            sock_num: A integer of requested number of sockets.
            is_echo: A bool of whether this command is an echo (send after receiving CONT).
        """
        command.put(fileSocket.recv_cont, self.main_queue, sock_num, is_echo)
        self.pause()

    def recv_send(self, filename, block_num, file_size):
        """Receive SEND command from another socket.

        Args:
            filename: A string of the relative path of the file.
            block_num: A integer of the number of split blocks.
            file_size: A integer of the size of the file.
        """
        # Receive a SEND command as the start mark of a block.
        command.put(fileSocket.recv_send, self.main_queue, filename, block_num)
        self.filename = filename
        self.fw = fileLoader.fileWriter(filename, file_size)

    def recv_pakg(self, position, data):
        """Receive PAKG command from another socket.

        Args:
            position: A integer of the written position in the file.
            data: A bytes of the file data.
        """
        # If the _protocol is in base64 style, the following code is needed to decode the string of
        # file data into bytes.
        # data = base64.b64decode(data)
        self.fw.write(position, data)

    def recv_vrfy(self):
        """Receive VRFY command from another socket.
        """
        # Receive a VERY command as the end mark of a block.
        self.fw.close()
        command.put(fileSocket.recv_vrfy, self.main_queue, self.filename)
        self.pause()


class recvSocket(object):
    """A subprocess-based class to send messages.

    Attributes:
        thread_list: A list of the _recvThread sub-threads.
        recv_addr: A address-like set of the local ip and port.
        main_queue: A queue to transmit message from fileSocket.recvSocket to fileSocket.fileSocket.
        recv_queue: A queue to transmit message to fileSocket.recvSocket.
        recv_sock: A socket to bind the local address and listen connections.
    """
    thread_list = list()

    def __init__(self, recv_addr, main_queue, recv_queue):
        self.recv_addr = recv_addr
        self.main_queue = main_queue
        self.recv_queue = recv_queue

        self.recv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.recv_sock.bind(self.recv_addr)
        self.recv_sock.listen()

    def start(self):
        """Start function for multiprocess.
        """
        while True:
            self.get_command()

    def get_command(self):
        """Get command from recv_queue.
        """
        command.get(self, self.recv_queue)

    def init_thread(self, num):
        """Initiate the socket threads for receiving message.

        Args:
            num: A integer of the number of initiated threads.
        """
        for i in range(num):
            self.thread_list.append(_recvThread(self.recv_sock, self.main_queue))
            self.thread_list[-1].start()


class fileSocket(object):
    """The main class for file sharing.

    Attributes:
        init_sock_num: A integer of the number of initial sockets.
        main_queue: A queue to transmit message to fileSocket.fileSocket.
        send_queue: A queue to transmit message to fileSocket.sendSocket.
        recv_queue: A queue to transmit message to fileSocket.recvSocket.
        ip: A string of the IPv4 host of the target socket.
        port: A integer of the port shared by local and other socket.
        recv_addr: A address-like set of the local ip and port.
        send_addr: A address-like set of the other ip and port.
        recv_dict: A inter-process dictionary of relative filepath (keys) and file_info (values) to
        determine whether the files are from other devices.
        my_send_socket: A instance of fileSocket.sendSocket.
        my_recv_socket: A instance of fileSocket.recvSocket.
        my_file_scanner: A instance of fileScanner.fileScanner.
        send_subproc: A process of my_send_socket.
        recv_subproc: A process of my_recv_socket.
        scan_subproc: A process of my_file_scanner.
    """
    init_sock_num = 1

    main_queue = multiprocessing.Queue()
    send_queue = multiprocessing.Queue()
    recv_queue = multiprocessing.Queue()

    def __init__(self, ip, port, sock_num=1, share_folder='./share'):
        self.port = port
        self.sock_num = sock_num
        self.recv_addr = ('', port)
        self.send_addr = (ip, port)
        self.recv_dict = multiprocessing.Manager().dict()

        self.my_send_socket = sendSocket(self.send_addr, self.send_queue)
        self.my_recv_socket = recvSocket(self.recv_addr, self.main_queue, self.recv_queue)
        self.my_file_scanner = fileScanner.fileScanner(share_folder, self.main_queue, self.recv_dict)

        self.send_subproc = multiprocessing.Process(target=self.my_send_socket.start)
        self.recv_subproc = multiprocessing.Process(target=self.my_recv_socket.start)
        self.scan_subproc = multiprocessing.Process(target=self.my_file_scanner.start)

        # All the subprocesses are daemon process of the main process.
        self.send_subproc.daemon = True
        self.recv_subproc.daemon = True
        self.scan_subproc.daemon = True

    def start(self):
        """Start function for file sharing.
        """
        self.scan_subproc.start()
        self.recv_subproc.start()
        self.send_subproc.start()
        self.connect()
        while True:
            self.get_command()

    def get_command(self):
        """Get command from main_queue.
        """
        command.get(self, self.main_queue)

    def connect(self):
        """Initiate the connection with other sockets.
        """
        self.init_thread(self.init_sock_num)
        self.send_cont(is_echo=False)

    def init_thread(self, sock_num):
        """Initiate the socket threads for sending and receiving message.

        Args:
            sock_num: A integer of the number of initiated threads.
        """
        command.put(recvSocket.init_thread, self.recv_queue, sock_num)
        command.put(sendSocket.init_thread, self.send_queue, sock_num)

    def send_cont(self, is_echo):
        """Give command to sendSocket to send CONT command.

        Args:
            is_echo: A bool of whether this command is an echo (send after receiving CONT).
        """
        command.put(sendSocket.send_cont, self.send_queue, self.sock_num, is_echo)

    def send_file(self, filename):
        """Give command to sendSocket to send a file.

        Args:
            filename: A string of the relative path of the file.
        """
        command.put(sendSocket.send_file, self.send_queue, filename)

    def recv_cont(self, sock_num, is_echo):
        """Receive CONT command from another socket. Initiate threads and echo CONT command if
        the received CONT is not echo.

        Args:
            sock_num: A integer of requested number of sockets.
            is_echo: A bool of whether this command is an echo (send after receiving CONT).
        """
        # TODO(Zirui): The reconnection will cause duplicate initiation of socket threads.
        self.init_thread(sock_num)
        if not is_echo:
            self.send_cont(is_echo=True)

    def recv_send(self, filename, block_num):
        """Receive SEND command from another socket. Mark the file into a transmission status.

        Args:
            filename: A string of the relative path of the file.
            block_num: A integer of the number of split blocks.
        """
        if filename not in self.recv_dict.keys():
            self.recv_dict[filename] = block_num

    def recv_vrfy(self, filename):
        """Receive VRFY command from another socket. Update the received block number.

        Args:
            filename: A string of the relative path of the file.
        """
        self.recv_dict[filename] -= 1
        if self.recv_dict[filename] == 0:
            self.recv_dict[filename] = fileLoader.get_file_info(filename)


class command(object):
    """A static class to put and get command from a queue.
    """
    @classmethod
    def get(cls, self, queue, retry=None):
        """Get message from a given queue.

        Args:
            self: A instance of the class which the function is from.
            queue:  A queue to read message from.
            retry: None or Exception to retry condition.
        """
        cmd = queue.get(block=True)
        if retry is None:
            cls.run(self, cmd)
        else:
            try:
                cls.run(self, cmd)
            except retry as e:
                queue.put(cmd)

    @staticmethod
    def put(func, queue, *args):
        """Put Message into a given queue.

        Args:
            func: A function to run by message receiver.
            queue: A queue to put message into.
            *args: Arguments for the given function.
        """
        queue.put((func, *args), block=True)

    @staticmethod
    def run(self, cmd):
        """Run the function in the message.

        Args:
            self: A instance of the class which the function is from.
            cmd: A set of values in the message.
        """
        cmd[0](self, *cmd[1:])


class _protocol(object):
    """A common-used class to pack some types of arguments into formatted bytes and unpack
    formatted bytes in reverse.

    Attributes:
        code: A enum-like class of defined code types.
        max_param: The Maximum of the given arguments.
        order_mark: A string of indicator for byte order, size and alignment of the packed data
        byte_mark: A string of a placeholder for bytes in format part.
        string_mark: A string of a placeholder for string in format part.
        code_format: A string of the struct format for code.
        guide_format: A string of the struct format for guide package.
    """
    class code:
        CONT = 'CONT'
        SEND = 'SEND'
        PAKG = 'PAKG'
        VRFY = 'VRFY'

    max_param = 8
    order_mark = '!'    # For network (= big-endian).
    byte_mark = '#'
    string_mark = '$'
    code_format = order_mark + '4s'
    guide_format = order_mark + 'I'

    @classmethod
    def pack(cls, code, *args):
        """Pack the given code and arguments into bytes.

        Args:
            code: A _protocol.code of the package code.
            *args: The arguments of the package.

        Returns:
            A bytes of packed code and arguments.

        Raises:
            Exception: A custom exception to pack incompatible values.
        """
        package = bytes()
        num_package = bytes()
        str_package = bytes()
        s_format = str()

        # Pack the values by their types and the format is recorded.
        for i in args:
            if isinstance(i, int):
                s_format += 'I'
                num_package += struct.pack(cls.order_mark + 'I', i)
            elif isinstance(i, float):
                s_format += 'd'
                num_package += struct.pack(cls.order_mark + 'd', i)
            elif isinstance(i, bool):
                s_format += '?'
                num_package += struct.pack(cls.order_mark + '?', i)
            # The length of string and bytes is recorded in the format.
            elif isinstance(i, str):
                s_format += cls.string_mark
                num_package += struct.pack(cls.order_mark + 'I', len(i))
                str_package += struct.pack(cls.order_mark + str(len(i)) + 's', i.encode())
            elif isinstance(i, bytes):
                s_format += cls.byte_mark
                num_package += struct.pack(cls.order_mark + 'I', len(i))
                str_package += struct.pack(cls.order_mark + str(len(i)) + 's', i)
            else:
                raise Exception("Pack Unknown Type: {} ({})".format(i, type(i)))

        # Join all parts of bytes into a complete package.
        s_format = s_format.ljust(cls.max_param, ' ')
        package += struct.pack(cls.code_format, code.encode())
        package += struct.pack(cls.order_mark + str(cls.max_param) + 's', s_format.encode())
        package += num_package
        package += str_package

        return package

    @classmethod
    def unpack(cls, package):
        """Unpack the given package bytes into code and arguments.

        Args:
            package: A bytes of formatted package.

        Returns:
            A set of unpacked code and arguments.
        """
        p_loader = cls._packageLoader(package)
        code = p_loader.unpack(cls.code_format)[0].decode()
        s_format = p_loader.unpack(cls.order_mark + str(cls.max_param) + 's')[0].decode()

        # Transfer the custom format into a struct-recognized format and unpack the package by
        # the transferred format.
        temp_format = s_format.strip()
        temp_format = temp_format.replace(cls.byte_mark, 'I')
        temp_format = temp_format.replace(cls.string_mark, 'I')
        args = list(p_loader.unpack(cls.order_mark + temp_format))

        # Unpack string and bytes by placing the byte_mark and string_mark in the custom format.
        # The integer provides the length of string or bytes for unpacking.
        for index, item in enumerate(s_format):
            if item == cls.byte_mark:
                args[index] = p_loader.unpack(cls.order_mark + str(args[index]) + 's')[0]
            if item == cls.string_mark:
                args[index] = p_loader.unpack(cls.order_mark + str(args[index]) + 's')[0].decode()

        # The code without brackets can run in Pycharm (Python 3.8), but not in virtual environment.
        return (code, *args)

    class _packageLoader(object):
        """A protected auxiliary class to unpack package.

        Attributes:
            package: A bytes of formatted package.
            pt: A integer of pointer of unpack position.
        """
        package = bytes()
        pt = 0

        def __init__(self, package):
            self.package = package

        def unpack(self, fmt):
            """Unpack the package by the given format from the position of pt.

            Args:
                fmt: A string of struct format to unpack.

            Returns:
                A set of unpacked values.
            """
            fmt_len = struct.calcsize(fmt)
            args = struct.unpack(fmt, self.package[self.pt: self.pt + fmt_len])
            self.pt += fmt_len
            return args


# Another implementation for packing code and arguments into a transferable binary.

# import base64
# import json
#
#
# class _protocol(object):
#     """A common-used class to pack some types of arguments into json-formatted string and unpack
#     json-formatted string in reverse.
#
#     Attributes:
#         code: A enum-like class of defined code types.
#         guide_format: A string of the struct format for guide package.
#     """
#     class code:
#         CONT = 'CONT'
#         SEND = 'SEND'
#         PAKG = 'PAKG'
#         VRFY = 'VRFY'
#         LINK = 'LINK'
#
#     guide_format = '!I'
#
#     class byteEncoder(json.JSONEncoder):
#         """A JSONEncoder-like class to encode data bytes into string by base64 encoding.
#         """
#         def default(self, obj):
#             if isinstance(obj, bytes):
#                 return base64.b64encode(obj).decode('utf-8')
#             return json.JSONEncoder.default(self, obj)
#
#     @classmethod
#     def pack(cls, code, *args):
#         """Pack the given code and arguments into json string.
#
#         Args:
#             code: A _protocol.code of the package code.
#             *args: The arguments of the package.
#
#         Returns:
#             A json string of packed code and arguments.
#         """
#         package = json.dumps((code, *args), cls=cls.byteEncoder).encode()
#         return package
#
#     @staticmethod
#     def unpack(package):
#         """Unpack the given json package into code and arguments.
#
#         Args:
#             package: A String of json-formatted package.
#
#         Returns:
#             A set of unpacked values.
#         """
#         args = json.loads(package.decode())
#         code = args[0]
#         args = args[1:]
#         # The code without brackets can run in Pycharm (Python 3.8), but not in virtual environment.
#         return (code, *args)
