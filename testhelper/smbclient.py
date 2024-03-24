import smbc  # type: ignore
import typing
import os


class SMBClient:
    """Use pysmbc to access the SMB server"""

    def __init__(self, hostname: str, share: str, username: str, passwd: str):
        self.server = hostname
        self.share = share
        self.username = username
        self.password = passwd
        self.rooturi = f"smb://{self.server}/{self.share}"

        def auth_cb(se, sh, w, u, p):
            return (w, self.username, self.password)

        self.ctx = smbc.Context(auth_fn=auth_cb)

    def disconnect(self):
        del self.ctx

    def readdir(self, path: str = "/") -> typing.List:
        uri = self.rooturi + path
        d_ent = self.ctx.opendir(uri)
        return [d_ent.name for d_ent in d_ent.getdents()]

    def _open(
        self, path: str, flags: int = os.O_RDWR, mode: int = 0o644
    ) -> typing.Any:
        uri = self.rooturi + path
        return self.ctx.open(uri, flags)

    def mkdir(self, dpath: str) -> None:
        self.ctx.mkdir(self.rooturi + dpath)

    def rmdir(self, dpath: str) -> None:
        self.ctx.rmdir(self.rooturi + dpath)

    def unlink(self, fpath: str) -> None:
        self.ctx.unlink(self.rooturi + fpath)

    def simple_write(self, fpath: str, teststr: str) -> None:
        f = self._open(fpath, os.O_WRONLY | os.O_CREAT)
        f.write(teststr)
        f.close()

    def simple_read(self, fpath: str) -> str:
        f = self._open(fpath)
        str = f.read()
        f.close()
        return str.decode("ascii")
