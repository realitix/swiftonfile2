import subprocess
import asyncio
from os import path
from oslo_log import log as logging

from swift.common.utils import config_true_value
from swiftonfile.watcher.generic import GenericWatcher, WatcherError


LOG = logging.getLogger(__name__)
LUSTRE_TYPE_CREATE   = "01CREAT"
LUSTRE_TYPE_HARDLINK = "03HLINK"
LUSTRE_TYPE_DELETE   = "06UNLNK"
LUSTRE_TYPE_RMDIR    = "07RMDIR"
LUSTRE_TYPE_RENAME   = "08RENME"
LUSTRE_TYPE_AVAILABLE = {
    LUSTRE_TYPE_CREATE,
    LUSTRE_TYPE_HARDLINK,
    LUSTRE_TYPE_DELETE,
    LUSTRE_TYPE_RMDIR,
    LUSTRE_TYPE_RENAME,
}

LUSTRE_TYPE_METHOD = {
    LUSTRE_TYPE_CREATE: "PUT",
    LUSTRE_TYPE_HARDLINK: "PUT",
    LUSTRE_TYPE_DELETE: "DELETE",
}

LUSTRE_TYPE_FORCE = (LUSTRE_TYPE_HARDLINK,)
CHANGELOG_QUANTITY_DEFAULT = 2000


def extract_fid(fid_raw):
    return fid_raw.split("=")[1]


class LustreError(WatcherError):
    pass


class FidNoSuchFileError(LustreError):
    pass


class NoContainerPathError(LustreError):
    pass


class LustreWatcher(GenericWatcher):
    def __init__(self, conf):
        super().__init__(conf)

        self.last_id = -1
        self.mdt = self.get_mdt()
        self.lustre_cl = self.get_lustre_client()
        self.force_sync_at_boot = config_true_value(
            conf.get("force_sync_at_boot", "false")
        )
        self.changelog_quantity = int(
            conf.get("watcher_lustre_changelog_quantity", CHANGELOG_QUANTITY_DEFAULT)
        )

    def get_mdt(self):
        mnt_point = path.join(self.node_folder, self.fs_name)
        r = subprocess.run(
            ["lfs", "mdts", mnt_point], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        if r.returncode != 0:
            raise LustreError(
                "Error with 'lfs mdts {}' command: {}".format(mnt_point, r.stderr)
            )

        if not r.stdout:
            raise LustreError(
                "Error with 'lfs mdts {}' command: No output".format(mnt_point)
            )

        return r.stdout.decode().splitlines()[1].split(" ")[1].split("_")[0]

    def get_lustre_client(self):
        """
        We want to be sure that Lustre changelog is configured with only one
        client. If several clX exists, we raise an exception. To check client
        existence, we launch changelog_clear command and ensure only one
        exists.
        When clearing, we use [endrec] of 1 to keep the changelog.
        If we get success code or error code of 22, it means user exists
        If we get error code of 2, it means user is not existent.
        """
        def test_client(client):
            r = subprocess.run(
                ["lfs", "changelog_clear", self.mdt, client, str(1)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            return r.returncode

        existings_cl = []
        # Check 32 first users
        for x in range(1, 32):
            cl = "cl{}".format(str(x))
            code = test_client(cl)
            if code == 13:
                raise LustreError("Permission error when retrieving Lustre client")
            if code != 2:
                existings_cl.append(cl)

        # Check only one cl exists
        if not existings_cl or len(existings_cl) > 1:
            raise LustreError(
                "Several Lustre client exist {} whereas it "
                "shouldn't ! Only one client is authorized".format(
                    " ".join(existings_cl)
                )
            )

        # store the client name
        lustre_cl = existings_cl[0]
        LOG.info(f"Lustre client init: {lustre_cl} selected")
        return lustre_cl

    async def fid_to_path(self, fid):
        # We have to take the parent fid because the child can be deleted
        # returncode = 2  => No such file or directory
        # returncode = 19 => No such device
        # returncode = 22 => Invalid argument
        proc = await asyncio.create_subprocess_shell(
            f"lfs fid2path {self.mdt} {fid}",
            stderr=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode == 2:
            raise FidNoSuchFileError()

        if proc.returncode != 0:
            raise LustreError("Problem with fid2path: {}".format(r.stderr))

        return stdout.decode().strip()

    def parent_path_to_container_path(self, parent_path):
        dirs = parent_path.strip("/").split("/")

        if len(dirs) < 2:
            raise NoContainerPathError("Bad container path: {}".format(parent_path))

        subdirectory = ""
        if len(dirs) > 2:
            subdirectory = path.join(*dirs[2:])

        return path.join(dirs[0], dirs[1]), subdirectory

    async def fid_to_container_path(self, fid):
        try:
            parent_path = await self.fid_to_path(extract_fid((fid)))
            return self.parent_path_to_container_path(parent_path)
        except (FidNoSuchFileError, NoContainerPathError):
            raise NoContainerPathError

    async def changelog_clear(self, endrec=1, client=None):
        """
        Return value:
        0 = client exists and changelog cleared
        2 = client doesn't exist
        22 = client exists but no endrec changelog
        """
        if client is None:
            client = self.lustre_cl

        proc = await asyncio.create_subprocess_shell(
            f"lfs changelog_clear {self.mdt} {client} {endrec}",
            stderr=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE
        )
        return await proc.wait()

    async def changelog_clear(self, endrec=1, client=None):
        """
        Return value:
        0 = client exists and changelog cleared
        2 = client doesn't exist
        22 = client exists but no endrec changelog
        """
        if client is None:
            client = self.lustre_cl

        proc = await asyncio.create_subprocess_shell(
            f"lfs changelog_clear {self.mdt} {client} {endrec}",
            stderr=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE
        )
        return await proc.wait()

    async def changelog(self, endrec=None, client=None):
        if client is None:
            client = self.lustre_cl

        cmd = f"lfs changelog {self.mdt} {client}"
        if endrec:
            cmd += f" {endrec}"

        proc = await asyncio.create_subprocess_shell(
            cmd,
            stderr=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            raise LustreError("Error during changelog command: {}".format(stderr))

        return stdout.decode().splitlines()

    async def get_lustre_default_data(self, container_path, subdirectory, filename, l_type):
        await self.queue.put(
            {
                "method": LUSTRE_TYPE_METHOD[l_type],
                "container_path": container_path,
                "filepath": path.join(subdirectory, filename),
                "force": True if l_type in LUSTRE_TYPE_FORCE else False,
            }
        )

    async def get_lustre_rmdir_data(self, container_path, subdirectory, filename):
        """ For rmdir, we set all files in the dir as delete """
        try:
            all_files_to_delete = await self.list_container_files(container_path, path.join(subdirectory, filename))
        except WatcherError:
            pass

        for filename_to_delete in all_files_to_delete:
            await self.queue.put(
                {
                    "method": LUSTRE_TYPE_METHOD[LUSTRE_TYPE_DELETE],
                    "container_path": container_path,
                    "filepath": filename_to_delete,
                    "force": False,
                }
            )

    async def get_lustre_rename_data(
            self, all_containers_path, container_path, subdirectory,
            filename, old_filename, old_container_path, old_subdirectory
    ):
        """
        The rename type is transformed to DELETE and CREATE
        with rename there are more entries.
        If it's a folder which is moved, it means that subdirectory is the
        parent of the folder and filename is the name of the folder. Same for
        old*.
        """
        file_to_add = []
        file_to_delete = []

        filename_fullpath = path.join(self.node_folder, self.fs_name, container_path, subdirectory, filename)

        if path.isdir(filename_fullpath):
            new_folder = path.join(subdirectory, filename)
            old_folder = path.join(old_subdirectory, old_filename)
            file_to_add = await self.get_local_files_in_container(container_path, new_folder)
            file_to_delete = [x.replace(new_folder, old_folder, 1) for x in file_to_add]
        else:
            file_to_add.append(path.join(subdirectory, filename))
            file_to_delete.append(path.join(old_subdirectory, old_filename))

        if old_container_path in all_containers_path:
            for filepath in file_to_delete:
                await self.queue.put(
                    {
                        "method": LUSTRE_TYPE_METHOD[LUSTRE_TYPE_DELETE],
                        "container_path": old_container_path,
                        "filepath": filepath,
                        "force": False,
                    }
                )

        if container_path in all_containers_path:
            for filepath in file_to_add:
                # We force the put (because file has already metadata)
                await self.queue.put(
                    {
                        "method": LUSTRE_TYPE_METHOD[LUSTRE_TYPE_CREATE],
                        "container_path": container_path,
                        "filepath": filepath,
                        "force": True,
                    }
                )

    async def get_changelog_data(self):
        all_containers_path = await self.get_all_local_containers_path()

        rec = None
        if self.last_id > 0:
            rec = self.last_id + self.changelog_quantity

        changelogs = await self.changelog(rec)

        for line in changelogs:
            LOG.debug(f"Lustre data: {line}")
            splitted_line = line.strip().split()

            l_id = int(splitted_line[0])
            l_type = splitted_line[1]
            force = False

            if l_type not in LUSTRE_TYPE_AVAILABLE:
                LOG.warning(
                    f"Found {l_type} type, you should set"
                    'mask to changelog_mask="CREAT UNLNK RENME RMDIR HLINK"'
                )
                continue

            filename = splitted_line[10]

            if l_id > self.last_id:
                self.last_id = l_id

            try:
                container_path, subdirectory = await self.fid_to_container_path(
                    splitted_line[9]
                )
            except NoContainerPathError:
                # The parent doesn't exist, we skip
                LOG.warning(f"No container path found for file {filename}")
                continue

            if l_type == LUSTRE_TYPE_RMDIR:
                if container_path in all_containers_path:
                    await self.get_lustre_rmdir_data(container_path, subdirectory, filename)

            elif l_type == LUSTRE_TYPE_RENAME:
                old_filename = splitted_line[13]

                try:
                    old_container_path, old_subdirectory = await self.fid_to_container_path(
                        splitted_line[12]
                    )
                except NoContainerPathError:
                    # The parent doesn't exist, we skip only the old
                    LOG.warning(f"No container path found for file {old_filename}")
                else:
                    await self.get_lustre_rename_data(all_containers_path, container_path, subdirectory, filename, old_filename, old_container_path, old_subdirectory)

            else:
                if container_path in all_containers_path:
                    await self.get_lustre_default_data(container_path, subdirectory, filename, l_type)

    async def get_data(self, iter_number):
        if self.force_sync_at_boot and iter_number == 0:
            self.last_id = 0  # Clear all changelog
            await super().get_data(iter_number)

        await self.get_changelog_data()
        await self.changelog_clear(endrec=self.last_id)
