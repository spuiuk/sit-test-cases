from smb.SMBConnection import SMBConnection  # type: ignore
from smb import smb_structs, base  # type: ignore
import typing
import io


class SMBClient:
    """Use pysmb to access the SMB server"""

    def __init__(self, hostname: str, share: str, username: str, passwd: str):
        self.server = hostname
        self.share = share
        self.username = username
        self.password = passwd
        self.connected = False
        self.connect()

    def connect(self) -> None:
        if self.connected:
            return
        try:
            self.ctx = SMBConnection(
                self.username,
                self.password,
                "smbclient",
                self.server,
                use_ntlm_v2=True,
            )
            self.ctx.connect(self.server)
            self.connected = True
        except base.SMBTimeout as error:
            raise IOError(f"failed to connect: {error}")

    def disconnect(self) -> None:
        self.connected = False
        try:
            self.ctx.close()
        except base.SMBTimeout as error:
            raise TimeoutError(f"disconnect: {error}")

    def listdir(self, path: str = "/") -> typing.List[str]:
        try:
            dentries = self.ctx.listPath(self.share, path)
        except smb_structs.OperationFailure as error:
            raise IOError(f"failed to readdir: {error}")
        except base.SMBTimeout as error:
            raise TimeoutError(f"listdir: {error}")
        except base.NotConnectedError as error:
            raise ConnectionError(f"listdir: {error}")

        return [dent.filename for dent in dentries]

    def mkdir(self, dpath: str) -> None:
        try:
            self.ctx.createDirectory(self.share, dpath)
        except smb_structs.OperationFailure as error:
            raise IOError(f"failed to mkdir: {error}")
        except base.SMBTimeout as error:
            raise TimeoutError(f"mkdir: {error}")
        except base.NotConnectedError as error:
            raise ConnectionError(f"mkdir: {error}")

    def rmdir(self, dpath: str) -> None:
        try:
            self.ctx.deleteDirectory(self.share, dpath)
        except smb_structs.OperationFailure as error:
            raise IOError(f"failed to rmdir: {error}")
        except base.SMBTimeout as error:
            raise TimeoutError(f"rmdir: {error}")
        except base.NotConnectedError as error:
            raise ConnectionError(f"rmdir: {error}")

    def unlink(self, fpath: str) -> None:
        try:
            self.ctx.deleteFiles(self.share, fpath)
        except smb_structs.OperationFailure as error:
            raise IOError(f"failed to unlink: {error}")
        except base.SMBTimeout as error:
            raise TimeoutError(f"unlink: {error}")
        except base.NotConnectedError as error:
            raise ConnectionError(f"unlink: {error}")

    def write(self, fpath: str, writeobj: typing.IO) -> None:
        try:
            self.ctx.storeFile(self.share, fpath, writeobj)
        except smb_structs.OperationFailure as error:
            raise IOError(f"failed in write_text: {error}")
        except base.SMBTimeout as error:
            raise TimeoutError(f"write_text: {error}")
        except base.NotConnectedError as error:
            raise ConnectionError(f"write: {error}")

    def read(self, fpath: str, readobj: typing.IO) -> None:
        try:
            self.ctx.retrieveFile(self.share, fpath, readobj)
        except smb_structs.OperationFailure as error:
            raise IOError(f"failed in read_text: {error}")
        except base.SMBTimeout as error:
            raise TimeoutError(f"read_text: {error}")
        except base.NotConnectedError as error:
            raise ConnectionError(f"read: {error}")

    def write_text(self, fpath: str, teststr: str) -> None:
        with io.BytesIO(teststr.encode()) as writeobj:
            self.write(fpath, writeobj)

    def read_text(self, fpath: str) -> str:
        with io.BytesIO() as readobj:
            self.read(fpath, readobj)
            ret = readobj.getvalue().decode("utf8")
        return ret
