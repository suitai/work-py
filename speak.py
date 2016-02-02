#!/usr/bin/env python
""" command to speak to a socket """

import os
import sys
import getopt
import yaml
import json
import socketalk
import getpass

CONFIG_FILE="speak.yaml"

def speak(message):
    if not os.path.exists(CONFIG_FILE):
        sys.exit("Error: Can not exists %s" % CONFIG_FILE)

    with open(CONFIG_FILE, 'r') as f:
        data = yaml.load(f)
    if "host" in data and "port" in data:
        host = data['host']
        port = data['port']
        print socketalk.Speak(message, host, port)


def usage():
    """ print usage """
    sys.exit("Usage: %s {--enqueue} [exec] \"message\"\n" % sys.argv[0])


def main():
    """ check argv """
    option = 'h'
    long_option = ["help", "enqueue"]
    message = {}
    queue_message = {}

    try:
        opts, args = getopt.getopt(sys.argv[1:], option, long_option)
    except getopt.GetoptError, detail:
        raise Exception('GetoptError: %s' % detail)

    # check opts
    for opt, arg in opts:
        if opt in ["-h", "--help"]:
            usage()
        elif opt == "--enqueue":
            queue_message = {'type': "enqueue"}

    # check args
    if args[0] == "exec":
        message = {'type': "execute",
                   'command': " ".join(args[1:]),
                   'uid': os.getuid(),
                   'gid': os.getgid(),
                   'username': getpass.getuser()}

    if queue_message and message:
        queue_message['item'] = message
        message = queue_message

    speak(json.dumps(message))


if __name__ == '__main__':
    main()
