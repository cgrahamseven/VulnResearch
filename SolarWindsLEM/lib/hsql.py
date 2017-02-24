'''
File: hsql.py 
Author: Chris Graham
Date: August 10, 2015
Details: This module implements a bare-bones HSQL client that is capable of logging in to a 
remote HSQL server and sending single SQL queries. 
'''
import socket
import struct

class HSQLClient:
    def __init__(self, host, port, user, passwd, database):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.database = database
        self.logged_in = False
        self.sock = None
    def _start_session(self):
        self.sock.send("\xff\xe1\x54\x70")
    def _parse_login_response(self, response):
        op = struct.unpack("c", response[0:1])
        if op[0] != "\x0b":
            return False
        # TODO: If login failed, report back message
        else:
            return True
    def _send_login_info(self):
        db_len = struct.pack(">L", len(self.database))
        user_len = struct.pack(">L", len(self.user))
        pass_len = struct.pack(">L", len(self.passwd))
        timezone_len = struct.pack(">L", len("America/Chicago"))
        pkt_len = 4 + len(db_len) + len(self.database) + \
        len(user_len) + len(self.user) + len(pass_len) + len(self.passwd) + \
        len(timezone_len) + len("America/Chicago") + len("\xff\xff\xb9\xb0")
        pkt_len = struct.pack(">L", pkt_len)
        login_pkt = "\x1f" + \
                    pkt_len + \
                    db_len + \
                    self.database + \
                    user_len + \
                    self.user + \
                    pass_len + \
                    self.passwd + \
                    timezone_len + \
                    "America/Chicago" + \
                    "\xff\xff\xb9\xb0"
        self.sock.send(login_pkt)
        self.sock.send("\x00")
        res = self.sock.recv(5)
        res_len = struct.unpack(">L", res[1:])
        res += self.sock.recv(res_len[0] - 4)
        # Responses end with a sungle null byte
        self.sock.recv(1)
        login_ok = self._parse_login_response(res)
        if login_ok:
            return True
        else:
            return False
    def single_query(self, sql_query):
        if not self.logged_in:
            raise ValueError("Not logged in to hsql server!")
        query_len = struct.pack(">L", len(sql_query))
        pkt_len = 4 + len("\x00\x00\x00\x00" + \
        "\x00\x00\x00\x00" + "\x00" + "\x00\x00\x00\x00") + \
        len(sql_query) + len("\x02\x00\x00\x02")
        pkt_len = struct.pack(">L", pkt_len)
        query_pkt = "\x22" + \
                    pkt_len + \
                    "\x00\x00\x00\x00" + \
                    "\x00\x00\x00\x00" + \
                    "\x00" + \
                    query_len + \
                    sql_query + \
                    "\x02\x00\x00\x02"
        self.sock.send(query_pkt)
        self.sock.send("\x00")
        res = self.sock.recv(5)
        res_len = struct.unpack(">L", res[1:])
        res += self.sock.recv(res_len[0] - 4)
        # Responses end with single null byte
        self.sock.recv(1)
        return res
    def close(self):
        if not self.logged_in:
            raise ValueError("Not logged in to hsql server!")
        self.sock.send("\x20\x00\x00\x00\x04\x00")
        res = self.sock.recv(5)
        res_len = struct.unpack(">L", res[1:])
        res += self.sock.recv(res_len[0] - 4)
        # Responses end with single null byte
        self.sock.recv(1)
        self.sock.close()
    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        self._start_session()
        self.logged_in = self._send_login_info()
        if not self.logged_in:
            self.sock.close()
            raise ValueError("Login failed to hsql server!")
