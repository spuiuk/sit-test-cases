import testhelper
from pathlib import Path


def test_read_yaml1():
    testinfo = testhelper.read_yaml("test-info1.yml")

    export1 = testinfo["shares"]["export1"]
    assert export1["server"] == "hostname1"
    assert export1["path"] == "/mnt/share/export1-cephfs-vfs"
    assert export1["backend"]["name"] == "cephfs.vfs"
    assert export1["backend"]["path"] == "/mnt/backend/export1"
    assert "user1" not in export1["users"]
    assert "test2" in export1["users"]
    assert export1["users"]["test2"] == "x"

    export2 = testinfo["shares"]["export2"]
    assert export2["server"] == "server_name"
    assert "path" not in export2
    assert export2["backend"]["name"] == "glusterfs"
    assert export2["backend"]["path"] is None
    assert "test2" not in export2["users"]
    assert "user2" in export2["users"]
    assert export2["users"]["user2"] == "user2password"


def test_read_yaml2():
    testinfo = testhelper.read_yaml("test-info2.yml")

    export = testinfo["shares"]["gluster-vol"]
    assert export["server"] == "192.168.123.10"
    assert "path" not in export
    assert export["backend"]["name"] == "glusterfs"
    assert "user1" not in export["users"]
    assert "test1" in export["users"]
    assert export["users"]["test1"] == "x"


def test_get_share():
    testinfo = testhelper.read_yaml("test-info1.yml")
    export2 = testhelper.get_share(testinfo, "export2")
    assert export2["server"] == "server_name"
    assert "path" not in export2
    assert export2["backend"]["name"] == "glusterfs"
    assert "test2" not in export2["users"]
    assert "user2" in export2["users"]
    assert export2["users"]["user2"] == "user2password"


def test_list_premounted():
    testinfo = testhelper.read_yaml("test-info1.yml")
    premounted = testhelper.get_premounted_shares(testinfo)
    assert len(premounted) == 1
    assert Path("/mnt/share/export1-cephfs-vfs") in premounted


def test_generate_exported_shares():
    testinfo = testhelper.read_yaml("test-info1.yml")
    arr = []
    for sharename in testhelper.get_exported_shares(testinfo):
        share = testhelper.get_share(testinfo, sharename)
        arr.append((share["server"], share["name"]))
    assert len(arr) == 1
    assert ("server_name", "export2") in arr


def test_get_extra_configuration():
    testinfo = testhelper.read_yaml("test-info1.yml")
    sg = testhelper.get_extra_configuration(testinfo, "supplementary_group")
    assert sg == "test_sg", "Could not read extra configuration"
    other = testhelper.get_extra_configuration(testinfo, "other")
    assert other is None, "Incorrectly read other from extra configuration"

    # test-info2.yml does not have extra section
    testinfo = testhelper.read_yaml("test-info2.yml")
    sg = testhelper.get_extra_configuration(testinfo, "supplementary_group")
    assert sg is None, "Could not read extra configuration"
