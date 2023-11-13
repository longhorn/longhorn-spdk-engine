import socket

def get_pod_ip():
    hostname = socket.gethostname()
    addr_info = socket.getaddrinfo(hostname, None)

    pod_ip = ""
    for addr in addr_info:
        if addr[0] == socket.AF_INET and addr[4][0] != "127.0.0.1":
            pod_ip = addr[4][0]
            break

    return pod_ip
