"""Functions for connecting to file or object storage systems.

Each function takes a connection_info parameter which is a dict identifying the system to
connect to. It should always have a "system_type" member identifying one of the following
supported systems:
    - "sftp" for paramiko.client.SSHClient.connect
    - "minio" for minio.Minio
    - "s3" for boto3.session.Session
    - "smb" for smb.SMBConnection

The remaining KVs of connection_info should map directly to the keyword arguments used in calling
the constructor indicated in the list above, with some exceptions:
    - For sftp, the pkey arg can just contain the key itself as a string (you can also supply a
        pkey_password arg). Will try all supported key types until it finds the one that works.
        You can also supply a known_hosts arg indicating a Prefect KV Store key whose value is a
        list of lines from a known_hosts file to use in invoking the SSHClient.load_host_keys
        function.
    - For minio, must include an additional "bucket" argument for the bucket name. Also, if
        "secure" is omitted, it defaults to True.
    - For smb, must include a "port" arg. The service name should be specified by the first element
        in the file path, preceded by a "/". IP address and "my_name" are automatically derived.
"""

import io
import uuid
from typing import BinaryIO, Union
import os
import stat
import logging
from contextlib import contextmanager
import socket

from paramiko.client import SSHClient
from paramiko.dsskey import DSSKey
from paramiko.rsakey import RSAKey
from paramiko.ecdsakey import ECDSAKey
from paramiko.ed25519key import Ed25519Key
from paramiko.ssh_exception import SSHException, AuthenticationException
from minio import Minio
from minio.error import S3Error
import boto3
import botocore
from smb.SMBConnection import SMBConnection
from smb.base import OperationFailure

from .util import sizeof_fmt, reveal_secrets, SB_LOGGER

# pylint:disable=not-callable


def get(object_name: str, connection_info: dict) -> bytes:
    """Returns the bytes content for the given file/object on the identified system. Raises
    FileNotFoundError if the file could not be found."""

    info = reveal_secrets(connection_info)
    function = _switch(info, sftp=sftp_get, minio=minio_get, s3=s3_get, smb=smb_get)
    return function(object_name, info)


def put(
    binary_object: Union[BinaryIO, bytes], object_name: str, connection_info: dict
) -> None:
    """Writes a binary file-like object or bytes string with the given name to the identified
    system."""

    info = reveal_secrets(connection_info)
    if not hasattr(binary_object, "read"):
        binary_object = io.BytesIO(binary_object)
    binary_object.seek(0)
    function = _switch(info, sftp=sftp_put, minio=minio_put, s3=s3_put, smb=smb_put)
    function(binary_object, object_name, info)


def remove(object_name: str, connection_info: dict) -> None:
    """Removes the identified file/object."""

    info = reveal_secrets(connection_info)
    function = _switch(
        info, sftp=sftp_remove, minio=minio_remove, s3=s3_remove, smb=smb_remove
    )
    function(object_name, info)


def list_names(connection_info: dict, prefix: str = None) -> list[str]:
    """Returns a list of object or file names in the given folder. Filters by object name prefix,
    which includes directory path for file systems. Folders are not included; non-recursive.
    """

    info = reveal_secrets(connection_info)
    function = _switch(info, sftp=sftp_list, minio=minio_list, s3=s3_list, smb=smb_list)
    if prefix:
        return function(info, prefix)
    return function(info)


# Utility function


def _switch(connection_info, **kwargs):
    for key, value in kwargs.items():
        if connection_info["system_type"] == key:
            del connection_info["system_type"]
            return value
    raise ValueError(f'System type "{connection_info["system_type"]}" is not supported')


# SFTP functions


def _make_ssh_key(connection_info):
    if "pkey" in connection_info:
        if "pkey_password" in connection_info:
            password = connection_info["pkey_password"]
            del connection_info["pkey_password"]
        else:
            password = None
        for key_type in [Ed25519Key, ECDSAKey, RSAKey, DSSKey]:
            try:
                pkey = key_type.from_private_key(
                    io.StringIO(connection_info["pkey"]), password
                )
                SB_LOGGER.info("SFTP: Loaded SSH private key using class %s", key_type)
                connection_info["pkey"] = pkey
                return
            except SSHException:
                pass
        raise ValueError(
            'connection_info["pkey"] could not be loaded using any Paramiko private '
            "key class: Verify this value gives the contents of a valid private key "
            'file and the password in connection_info["pkey_password"] (if applicable)'
            " is correct"
        )


def _load_known_hosts(ssh_client, connection_info):
    if "known_hosts" in connection_info:
        hosts_filename = os.path.expanduser(
            f"~/.ssh/prefect_known_hosts_{uuid.uuid4()}"
        )
        try:
            os.mkdir(os.path.expanduser("~/.ssh/"))
        except FileExistsError:
            pass
        with open(hosts_filename, "w", encoding="ascii") as fileobj:
            fileobj.write("\n".join(connection_info["known_hosts"]))
        ssh_client.load_host_keys(hosts_filename)
        os.remove(hosts_filename)
        del connection_info["known_hosts"]
        if "look_for_keys" not in connection_info:
            connection_info["look_for_keys"] = False
    else:
        ssh_client.load_system_host_keys()


def _sftp_chdir(sftp, remote_directory):
    if remote_directory == "/":
        # absolute path so change directory to root
        sftp.chdir("/")
        return
    if remote_directory == "":
        # top-level relative directory must exist
        return

    try:
        sftp.chdir(remote_directory)  # sub-directory exists
    except IOError:
        dirname, basename = os.path.split(remote_directory.rstrip("/"))

        _sftp_chdir(sftp, dirname)  # make parent directories

        try:
            sftp.mkdir(basename)  # sub-directory missing, so created it
        except OSError:
            pass

        sftp.chdir(basename)


@contextmanager
def _sftp_connection(ssh_client, connection_info):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    logger = logging.getLogger("paramiko")
    level = logger.level
    try:
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        ssh_client.connect(**connection_info)
        yield ssh_client.open_sftp()
    except AuthenticationException:
        SB_LOGGER.error(
            "Paramiko SSH Authentication failed. You may need to specify 'disabled_algorithms'. "
            "See logs:\n\n%s",
            stream.getvalue(),
        )
        raise
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)
        if ssh_client:
            ssh_client.close()


def sftp_get(file_path: str, connection_info: dict) -> bytes:
    """Returns the bytes content for the file at the given path from an SFTP server."""

    SB_LOGGER.info(
        "SFTP: Getting file %s from %s", file_path, connection_info["hostname"]
    )
    _make_ssh_key(connection_info)
    ssh = SSHClient()
    _load_known_hosts(ssh, connection_info)
    with _sftp_connection(ssh, connection_info) as sftp:
        out = io.BytesIO()
        try:
            sftp.getfo(file_path, out)
        except IOError as err:
            raise FileNotFoundError(
                f'File {file_path} not found on {connection_info["hostname"]}'
            ) from err
        out = out.getvalue()
    SB_LOGGER.info("SFTP: Got %s file", sizeof_fmt(len(out)))
    return out


def sftp_put(file_object: BinaryIO, file_path: str, connection_info: dict) -> None:
    """Writes a file-like object or bytes string to the given path on an SFTP server."""

    size = file_object.seek(0, 2)
    file_object.seek(0)
    SB_LOGGER.info(
        "SFTP: Putting file %s (%s) onto %s",
        file_path,
        sizeof_fmt(size),
        connection_info["hostname"],
    )
    _make_ssh_key(connection_info)
    ssh = SSHClient()
    _load_known_hosts(ssh, connection_info)
    with _sftp_connection(ssh, connection_info) as sftp:
        _sftp_chdir(sftp, os.path.dirname(file_path))
        # Confirming can throw an error on CUTransfer where files are moved immediately
        sftp.putfo(file_object, os.path.basename(file_path), confirm=False)


def sftp_remove(file_path: str, connection_info: dict) -> None:
    """Removes the identified file."""

    SB_LOGGER.info(
        "SFTP: Removing file %s from %s", file_path, connection_info["hostname"]
    )
    _make_ssh_key(connection_info)
    ssh = SSHClient()
    _load_known_hosts(ssh, connection_info)
    with _sftp_connection(ssh, connection_info) as sftp:
        sftp.remove(file_path)


def sftp_list(connection_info: dict, file_prefix: str = "./") -> list[str]:
    """Returns a list of filenames for files with the given path prefix. Only the filenames are
    returned, without folder paths."""

    directory = os.path.dirname(file_prefix)
    prefix = os.path.basename(file_prefix)
    SB_LOGGER.info(
        "SFTP: Finding files at '%s' with prefix '%s' on %s",
        directory,
        prefix,
        connection_info["hostname"],
    )
    _make_ssh_key(connection_info)
    ssh = SSHClient()
    _load_known_hosts(ssh, connection_info)
    with _sftp_connection(ssh, connection_info) as sftp:
        out = [
            i.filename
            for i in sftp.listdir_attr(directory)
            if stat.S_ISREG(i.st_mode) and i.filename.startswith(prefix)
        ]
    SB_LOGGER.info("SFTP: Found %s files", len(out))
    return out


# Minio functions


def minio_get(object_name: str, connection_info: dict) -> bytes:
    """Returns the bytes content for the given object in a Minio bucket."""

    if "secure" not in connection_info:
        connection_info["secure"] = True
    bucket = connection_info["bucket"]
    del connection_info["bucket"]
    SB_LOGGER.info(
        "Minio: Getting object %s from bucket %s on %s",
        object_name,
        bucket,
        connection_info["endpoint"],
    )
    minio = Minio(**connection_info)
    try:
        response = minio.get_object(bucket, object_name)
        out = response.data
    except S3Error as err:
        if err.code == "NoSuchKey":
            raise FileNotFoundError(
                f"Object {object_name} not found on {bucket}"
            ) from err
        raise
    finally:
        try:
            response.close()
            response.release_conn()
        except NameError:
            # response never got defined
            pass
    SB_LOGGER.info("Minio: Got %s object", sizeof_fmt(len(out)))
    return out


def minio_put(binary_object: BinaryIO, object_name: str, connection_info: dict) -> None:
    """Puts the given BinaryIO object into a Minio bucket. Any additional keyword arguments are
    passed to the Minio.put_object function."""

    if "secure" not in connection_info:
        connection_info["secure"] = True
    bucket = connection_info["bucket"]
    del connection_info["bucket"]
    size = binary_object.seek(0, 2)
    binary_object.seek(0)
    SB_LOGGER.info(
        "Minio: Putting object %s (%s) into bucket %s on %s",
        object_name,
        sizeof_fmt(size),
        bucket,
        connection_info["endpoint"],
    )
    minio = Minio(**connection_info)
    minio.put_object(
        bucket_name=bucket, object_name=object_name, data=binary_object, length=size
    )


def minio_remove(object_name: str, connection_info: dict) -> None:
    """Removes the identified object from a Minio bucket."""

    if "secure" not in connection_info:
        connection_info["secure"] = True
    bucket = connection_info["bucket"]
    del connection_info["bucket"]
    SB_LOGGER.info(
        "Minio: Removing object %s from bucket %s on %s",
        object_name,
        bucket,
        connection_info["endpoint"],
    )
    minio = Minio(**connection_info)
    minio.remove_object(bucket, object_name)


def minio_list(connection_info: dict, prefix: str = "") -> list[str]:
    """Returns a list of object names with the given prefix in a Minio bucket; non-recursive."""

    if "secure" not in connection_info:
        connection_info["secure"] = True
    bucket = connection_info["bucket"]
    del connection_info["bucket"]
    SB_LOGGER.info(
        'Minio: Finding files with prefix "%s" in bucket %s on %s',
        prefix,
        bucket,
        connection_info["endpoint"],
    )
    minio = Minio(**connection_info)
    out = [
        os.path.basename(i.object_name)
        for i in minio.list_objects(bucket, prefix=prefix)
    ]
    SB_LOGGER.info("Minio: Found %s files", len(out))
    return out


# S3 functions


def s3_get(object_key: str, connection_info: dict) -> bytes:
    """Returns the bytes content for the given object in an Amazon S3 bucket."""

    bucket = connection_info["bucket"]
    del connection_info["bucket"]
    SB_LOGGER.info("Amazon S3: Getting object %s from bucket %s", object_key, bucket)
    session = boto3.session.Session(**connection_info)
    s3res = session.resource("s3")
    obj = s3res.Object(bucket, object_key)
    data = io.BytesIO()
    try:
        obj.download_fileobj(data)
    except botocore.exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "404":
            raise FileNotFoundError(
                f"Object {object_key} not found in {bucket}"
            ) from err
        raise
    out = data.getvalue()
    SB_LOGGER.info("Amazon S3: Got %s object", sizeof_fmt(len(out)))
    return out


def s3_put(
    binary_object: BinaryIO,
    object_key: str,
    connection_info: dict,
    ExtraArgs: dict = None,
) -> None:
    """Puts the given BinaryIO object into an Amazon S3 bucket. The optional ExtraArgs parameter
    is passed to upload_fileobj if provided."""

    # pylint:disable=invalid-name
    bucket = connection_info["bucket"]
    del connection_info["bucket"]
    size = binary_object.seek(0, 2)
    binary_object.seek(0)
    SB_LOGGER.info(
        "Amazon S3: Putting object %s (%s) into bucket %s",
        object_key,
        sizeof_fmt(size),
        bucket,
    )
    session = boto3.session.Session(**connection_info)
    s3res = session.resource("s3")
    bucket_res = s3res.Bucket(bucket)
    try:
        bucket_res.load()
    except botocore.exceptions.ClientError as err:
        if err.response["Error"]["Code"] == "404":
            bucket_res.create()
        else:
            raise
    bucket_res.upload_fileobj(binary_object, object_key, ExtraArgs=ExtraArgs)


def s3_remove(object_key: str, connection_info: dict, VersionId: str = None) -> None:
    """Removes the identified object from an Amazon S3 bucket. The optional VersionId parameter
    is passed to the delete method if provided (otherwise, the null version is deleted.
    """

    # pylint:disable=invalid-name
    bucket = connection_info["bucket"]
    del connection_info["bucket"]
    SB_LOGGER.info("Amazon S3: Removing object %s from bucket %s", object_key, bucket)
    session = boto3.session.Session(**connection_info)
    s3res = session.resource("s3")
    obj = s3res.Object(bucket, object_key)
    obj.delete(VersionId=VersionId)


def s3_list(connection_info: dict, Prefix: str = "") -> list[str]:
    """Returns a list of object names with the given prefix in an Amazon S3 bucket;
    non-recursive."""

    # pylint:disable=invalid-name
    bucket = connection_info["bucket"]
    del connection_info["bucket"]
    SB_LOGGER.info(
        'Amazon S3: Finding files with prefix "%s" in bucket %s', Prefix, bucket
    )
    session = boto3.session.Session(**connection_info)
    s3res = session.resource("s3")
    bucket = s3res.Bucket(bucket)
    out = [os.path.basename(i.key) for i in bucket.objects.filter(Prefix=Prefix)]
    SB_LOGGER.info("Amazon S3: Found %s files", len(out))
    return out


# SMB functions


def smb_get(file_path: str, connection_info: dict) -> bytes:
    """Returns the bytes content for the given file on an SMB server."""

    server_ip = socket.gethostbyname(connection_info["remote_name"])
    port = connection_info.pop("port", None)
    if not file_path.startswith("/"):
        raise ValueError(
            "File path must start with '/' followed by the SMB service name"
        )
    service_name = file_path.split("/")[1]
    file_path = file_path.removeprefix(f"/{service_name}")
    SB_LOGGER.info(
        "SMB: Getting file %s from %s on %s",
        file_path,
        service_name,
        connection_info["remote_name"],
    )
    with SMBConnection(
        my_name=socket.gethostname(),
        sign_options=SMBConnection.SIGN_WHEN_SUPPORTED,
        use_ntlm_v2=True,
        **connection_info,
    ) as conn:
        if not conn.connect(server_ip, port):
            raise RuntimeError(
                f'Unable to connect to {connection_info["remote_name"]} ({server_ip}): '
                "Authentication failed"
            )
        out = io.BytesIO()
        try:
            conn.retrieveFile(service_name, file_path, out)
        except OperationFailure as err:
            raise FileNotFoundError(
                f'File {file_path} not found in {service_name} on {connection_info["remote_name"]}'
            ) from err
    out = out.getvalue()
    SB_LOGGER.info("SMB: Got %s file", sizeof_fmt(len(out)))
    return out


def smb_put(
    file_object: BinaryIO,
    file_path: str,
    connection_info: dict,
) -> None:
    """Writes a file-like object or bytes string tot he given path on an SMB server."""

    size = file_object.seek(0, 2)
    file_object.seek(0)
    server_ip = socket.gethostbyname(connection_info["remote_name"])
    port = connection_info.pop("port", None)
    if not file_path.startswith("/"):
        raise ValueError(
            "File path must start with '/' followed by the SMB service name"
        )
    service_name = file_path.split("/")[1]
    file_path = file_path.removeprefix(f"/{service_name}")
    SB_LOGGER.info(
        "SMB: Putting file %s (%s) in %s on %s",
        file_path,
        sizeof_fmt(size),
        service_name,
        connection_info["remote_name"],
    )
    with SMBConnection(
        my_name=socket.gethostname(),
        sign_options=SMBConnection.SIGN_WHEN_SUPPORTED,
        use_ntlm_v2=True,
        **connection_info,
    ) as conn:
        if not conn.connect(server_ip, port):
            raise RuntimeError(
                f'Unable to connect to {connection_info["remote_name"]} ({server_ip}): '
                "Authentication failed"
            )
        try:
            conn.createDirectory(service_name, os.path.dirname(file_path))
        except OperationFailure:
            # directory already exists
            pass
        conn.storeFile(service_name, file_path, file_object)


def smb_remove(file_path: str, connection_info: dict) -> None:
    """Removes the identified file"""

    server_ip = socket.gethostbyname(connection_info["remote_name"])
    port = connection_info.pop("port", None)
    if not file_path.startswith("/"):
        raise ValueError(
            "File path must start with '/' followed by the SMB service name"
        )
    service_name = file_path.split("/")[1]
    file_path = file_path.removeprefix(f"/{service_name}")
    SB_LOGGER.info(
        "SMB: Removing file %s from %s on %s",
        file_path,
        service_name,
        connection_info["remote_name"],
    )
    with SMBConnection(
        my_name=socket.gethostname(),
        sign_options=SMBConnection.SIGN_WHEN_SUPPORTED,
        use_ntlm_v2=True,
        **connection_info,
    ) as conn:
        if not conn.connect(server_ip, port):
            raise RuntimeError(
                f'Unable to connect to {connection_info["remote_name"]} ({server_ip}): '
                "Authentication failed"
            )
        conn.deleteFiles(service_name, file_path)


def smb_list(connection_info: dict, prefix: str = "./") -> list[str]:
    """Returns a list of filenames for files with the given path prefix. Only the filenames are
    returned, without folder paths."""

    server_ip = socket.gethostbyname(connection_info["remote_name"])
    port = connection_info.pop("port", None)
    if not prefix.startswith("/"):
        raise ValueError("Prefix must start with '/' followed by the SMB service name")
    service_name = prefix.split("/")[1]
    prefix = prefix.removeprefix(f"/{service_name}")
    SB_LOGGER.info(
        "SMB: Finding files at '%s' in %s on %s",
        prefix,
        service_name,
        connection_info["remote_name"],
    )
    with SMBConnection(
        my_name=socket.gethostname(),
        sign_options=SMBConnection.SIGN_WHEN_SUPPORTED,
        use_ntlm_v2=True,
        **connection_info,
    ) as conn:
        if not conn.connect(server_ip, port):
            raise RuntimeError(
                f"Unable to connect to {connection_info['remote_name']} ({server_ip}): "
                "Authentication failed"
            )
        out = [
            i.filename
            for i in conn.listPath(
                service_name,
                path=os.path.dirname(prefix),
                pattern=f"{os.path.basename(prefix)}*",
            )
        ]
    SB_LOGGER.info("SMB: Found %s files", len(out))
    return out
