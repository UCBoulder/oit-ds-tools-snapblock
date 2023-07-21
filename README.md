# Snapblock

Snapblock abstracts data exchange with diverse external systems, such as object storage and databases, into atomic operations, enabling seamless and efficient interfacing defined by system-agnostic JSON blocks.

## Usage

`snapblock` is a versatile Python package that abstracts data operations with various external systems. For example, consider the `object_storage` module, used for interfacing with SFTP servers, SMB shares, S3 buckets, and more. The same four operations work similarly on all these systems: `get`, `put`, `remove`, and `list_names`.

Snapblock's atomic operations all require a JSON-ifiable `connection_info` dictionary, which specifies everything needed to connect to the system, such as hostname, username, password, etc. There are no external connection dependencies required, like a known_hosts file or a private key file: all this can be included in the connection block. This dictionary contains a `system_type` key specifying the kind of system to connect to (in this case, the type of object storage system), so you can easily change the target of your code from an SFTP server to an S3 bucket without recoding a thing.

To protect sensitive data, `snapblock` allows secrets to be stored securely and only revealed at runtime. Any string in the `connection_info` that starts with `<secret>` will be replaced by the corresponding secret value during execution. Right now, secrets can be pulled from Prefect if that package is available and otherwise will use environmental variables; more secret store options can be added over time.

Here's an example of how to use `snapblock` with an SFTP system that uses a private key file (whose contents could be stored in a `PRIVATE_KEY` environmental variable):

```python
from snapblock import object_storage

connection_info = {
    "pkey": "<secret>private_key",
    "hostname": "sftp.my_domain.com",
    "username": "my-username",
    "known_hosts": ["example-known-hosts-entry"],
    "system_type": "sftp",
}
object_name = "path/to/myfile.csv"
file_contents = object_storage.get(object_name, connection_info)
# data = pd.read_csv(file_contents)
```

This retrieves the content of the specified object from the SFTP system defined by the connection_info.

Similarly, you can store data, remove objects, or list all object names in the system. For more details, please check the function-specific API documentation.

## Supported System Types

`database.py`
 - Oracle
 - Postgres
 - ODBC
 - MySQL

`object_storage.py`
 - SFTP
 - SMB
 - S3
 - MinIO

Plus GraphQL and standard REST API endpoints.

## Installation

This isn't on PyPi yet, but you can install from the Github repo:

`pip install git+https://github.com/UCBoulder/oit-ds-tools-snapblock.git`
