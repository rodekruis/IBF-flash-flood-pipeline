from ftplib import FTP_TLS
from io import BytesIO


def download_file(host, username, remote_filepath, ftp_timeout: float = 10.0):
    memory_file = BytesIO()

    with FTP_TLS(host, timeout=ftp_timeout) as ftp:
        ftp.login(user=username, passwd=username)
        ftp.cwd(remote_filepath.parent.as_posix())

        ftp.retrbinary(f"RETR {remote_filepath.name}", memory_file.write)

    return (remote_filepath.name, memory_file)
