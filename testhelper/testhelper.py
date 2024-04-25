import yaml
import typing
import random
from pathlib import Path


def _get_default_backend(test_info: dict) -> str:
    return test_info.get("backend") or test_info.get("test_backend", "xfs")


def _get_default_server(test_info: dict) -> str:
    return (
        test_info.get("server")
        or test_info.get("public_interfaces", ["localhost"])[0]
    )


def _get_default_users(test_info: dict) -> typing.Dict[str, str]:
    default_users = test_info.get("users", {})
    if not default_users:
        for user in test_info.get("test_users", []):
            default_users[user["username"]] = user["password"]
    return default_users


def read_yaml(test_info_file):
    """Returns a dict containing the contents of the yaml file.

    Parameters:
    test_info_file: filename of yaml file.

    Returns:
    dict: The parsed test information yml as a dictionary.
    """
    with open(test_info_file) as f:
        test_info = yaml.load(f, Loader=yaml.FullLoader)

    shares = test_info.get("shares", {})

    # Copy exported_sharenames to shares
    # Todo - remove once sit-environment is updated
    for sharename in test_info.get("exported_sharenames", []):
        assert sharename not in shares, "Duplicate share name present"
        shares[sharename] = {}

    # Add missing fields with defaults
    # Todo : Remove old field names once sit-environment is updated
    default_backend = _get_default_backend(test_info)
    default_server = _get_default_server(test_info)
    default_users = _get_default_users(test_info)

    for sharename in shares:
        if shares[sharename] is None:
            shares[sharename] = {"name": sharename}
        share = shares[sharename]
        share.setdefault("name", sharename)
        share.setdefault("server", default_server)
        share.setdefault("users", default_users)
        share.setdefault("backend", {})
        share["backend"].setdefault("name", default_backend)
        share["backend"].setdefault("path", None)

    test_info["shares"] = shares

    return test_info


def gen_mount_params(
    host: str, share: str, username: str, password: str
) -> typing.Dict[str, str]:
    """Generate a dict of parameters required to mount a SMB share.

    Parameters:
    host: hostname
    share: exported share name
    username: username
    password: password for the user

    Returns:
    dict: mount parameters in a dict
    """
    ret = {
        "host": host,
        "share": share,
        "username": username,
        "password": password,
    }
    return ret


def get_mount_parameters(test_info: dict, share: str) -> typing.Dict[str, str]:
    """Get the default mount_params dict for a given share

    Parameters:
    test_info: Dict containing the parsed yaml file.
    share: The share for which to get the mount_params
    """
    s = get_share(test_info, share)
    server = s["server"]
    users = list(s["users"].keys())
    return gen_mount_params(
        server,
        share,
        users[0],
        s["users"][users[0]],
    )


def generate_random_bytes(size: int) -> bytes:
    """
    Creates sequence of semi-random bytes.

    A wrapper over standard 'random.randbytes()' which should be used in
    cases where caller wants to avoid exhausting of host's random pool (which
    may also yield high CPU usage). Uses an existing random bytes-array to
    re-construct a new one, double in size, plus up-to 1K of newly random
    bytes. This method creats only "pseudo" (or "semi") random bytes instead
    of true random bytes-sequence, which should be good enough for I/O
    integrity testings.
    """
    rba = bytearray(random.randbytes(min(size, 1024)))
    while len(rba) < size:
        rem = size - len(rba)
        rnd = bytearray(random.randbytes(min(rem, 1024)))
        rba = rba + rnd + rba
    return rba[:size]


def get_shares(test_info: dict) -> dict:
    """
    Get list of shares

    Parameters:
    test_info: Dict containing the parsed yaml file.
    Returns:
    list of dict of shares
    """
    return test_info["shares"]


def get_share(test_info: dict, sharename: str) -> dict:
    """
    Get share dict for a given sharename

    Parameters:
    test_info: Dict containing the parsed yaml file.
    sharename: name of the share
    Returns:
    dict of the share
    """
    shares = get_shares(test_info)
    assert sharename in shares.keys(), "Share not found"
    return shares[sharename]


def is_premounted_share(share: dict) -> bool:
    """
    Check if the share is a premounted share

    Parameters:
    share: dict of the share
    Returns:
    bool
    """
    mntdir = share.get("path")
    return mntdir is not None


def get_premounted_shares(test_info: dict) -> typing.List[Path]:
    """
    Get list of premounted shares

    Parameters:
    None
    Returns:
    list of paths with shares
    """
    share_values = get_shares(test_info).values()
    return [Path(s["path"]) for s in share_values if is_premounted_share(s)]


def get_exported_shares(test_info: dict) -> typing.List[str]:
    """Get the list of exported shares

    Parameters:
    test_info: Dict containing the parsed yaml file.
    Returns:
    list of exported shares
    """
    arr = []
    for share in get_shares(test_info).values():
        if not is_premounted_share(share):
            arr.append(share["name"])
    return arr


def get_extra_configuration(test_info: dict, extra_var: str) -> typing.Any:
    """Return value of config set in extra section

    Parameters:
    test_info: Dict containing the parsed yaml file
    extra_var: string containing extra config value required
    Returns:
    value set for the extra_var in the extra section
    """
    extra = test_info.get("extra")
    if extra is None:
        return None
    return extra.get(extra_var)
