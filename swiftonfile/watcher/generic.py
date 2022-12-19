import asyncio
import aiohttp
import os
from os import path
import requests
from time import sleep
from swiftonfile.swift.common.utils import is_tmp_obj
from oslo_log import log as logging


LOG = logging.getLogger(__name__)
WATCHER_INTERVAL_DEFAULT = 60  # One minute
WATCHER_NB_THREADS = 10


async def walk(path):
    return await asyncio.get_event_loop().run_in_executor(None, os.walk, path)


class WatcherError(Exception):
    pass


class GenericWatcher:
    def __init__(self, conf):
        self.swift_port = conf["bind_port"]
        self.node_folder = conf["devices"]
        self.watcher_interval = int(
            conf.get("watcher_interval", WATCHER_INTERVAL_DEFAULT)
        )
        self.watcher_nb_threads = int(
            conf.get("watcher_nb_threads", WATCHER_NB_THREADS)
        )

        LOG.info(f"Watcher interval: {self.watcher_interval}")

        folders = os.listdir(self.node_folder)
        if not folders or len(folders) > 1:
            raise WatcherError(
                "Must have only one node in {}.\
                Replication is handled by FS".format(
                    self.node_folder
                )
            )

        self.fs_name = folders[0]
        self.container_path_blacklist = []
        self.stop_watch = False  # To start the watch only one time
        self.queue = asyncio.Queue()

    def check_request_response(self, status_code, container_path):
        if status_code == 404:
            LOG.info(f"Blacklisting {container_path}")
            self.container_path_blacklist.append(container_path)
            raise WatcherError()
        elif status_code == 400:
            # This error happens when a new file is created by Swift
            # So it's not a problem
            pass
        elif status_code != 200:
            LOG.error(
                f"Unknow error: {response.status_code} "
                f"for container path {container_path}"
            )
            raise WatcherError()

    def check_container_path(self, container_path):
        if container_path in self.container_path_blacklist:
            LOG.warning(f"Trying to patch a blacklisted container {container_path}")
            raise WatcherError()

    async def send_patch(self, method, container_path, filepath, force_put=False):
        """
        force_put is used to force a put when renaming a file.
        In the server side, we don't refresh container if the file already has metadata.
        This is done to prevent over utilization of container API when this is Swift which
        create a new file. In the case of a rename, we need to force the put because
        the file already has metadata.
        """
        if method not in ("PUT", "DELETE"):
            raise WatcherError("Method {} unavailable".format(method))

        self.check_container_path(container_path)

        # Don't send tmp objects created by diskfile
        if is_tmp_obj(filepath):
            return

        url = "http://localhost:{port}/{fs_name}/{fullpath}".format(
            port=self.swift_port,
            fs_name=self.fs_name,
            fullpath=path.join(container_path, filepath),
        )

        async with aiohttp.request("patch",
            url, headers={"X-Patch-Method": method, "X-Patch-Force": str(force_put)}
        ) as response:
            status_code = response.status
            if status_code == 200:
                LOG.info(f"Send {method} to {url}")
        self.check_request_response(status_code, container_path)

    async def list_container_files(self, container_path, subfolder=None):
        self.check_container_path(container_path)

        if subfolder:
            container_path = path.join(container_path, subfolder)

        url = "http://localhost:{port}/{fs_name}/{container_path}".format(
            port=self.swift_port, fs_name=self.fs_name, container_path=container_path
        )

        # Use custom HTTP method LIST
        async with aiohttp.request("list", url) as response:
            status_code = response.status
            text = await response.text()

        self.check_request_response(status_code, container_path)

        return text.splitlines()

    async def get_all_local_containers_path(self):
        src_dir = path.join(self.node_folder, self.fs_name)
        results = []

        # Get all account folders in root
        account_folders = next(await walk(src_dir))[1]

        # In theses account folders, get folders level2
        for account_folder in account_folders:
            account_path = path.join(src_dir, account_folder)
            container_folders = next(await walk(account_path))[1]

            # In theses container folders, get only not blacklisted
            for container_folder in container_folders:
                container_path = path.join(account_folder, container_folder)
                if container_path not in self.container_path_blacklist:
                    results.append(container_path)

        return results

    async def get_local_files_in_container(self, container_path, subfolder=None):
        """
        All files are returned included in subdirectory
        """
        cpath = path.join(self.node_folder, self.fs_name, container_path)
        cpath_search = cpath
        if subfolder:
            cpath_search = path.join(cpath, subfolder)
        local_files = []
        for root, dirs, files in await walk(cpath_search):
            for f in files:
                full_path = path.join(root, f)
                local_files.append(full_path[len(cpath) + 1 :])

        return local_files

    async def get_data(self, iter_number):
        """
        Compare all files on disk with file registered in each container

        Return data as array of dict containing:
            'method': 'PUT' or 'DELETE',
            'filepath': path to the file (from Account folder)
        """
        # TODO: problem with renaming file
        # When renaming, file will be deleted but not recreated
        # because we have to add the property "force" to the to_put entry.
        # But how to know that a file has been renamed ?

        # Get all local containers
        # Only like that: Account/Container/*only
        containers_path = await self.get_all_local_containers_path()

        container_to_add = []
        container_to_delete = []
        for cpath in containers_path:
            try:
                container_files = await self.list_container_files(cpath)
            except WatcherError:
                continue

            local_files = await self.get_local_files_in_container(cpath)

            # Check to add
            for local_file in local_files:
                if local_file not in container_files:
                    container_to_add.append({"cpath": cpath, "file": local_file})

            # Check to remove
            for container_file in container_files:
                if container_file not in local_files:
                    container_to_delete.append({"cpath": cpath, "file": container_file})

        for x in container_to_add:
            await self.queue.put({"method": "PUT", "container_path": x["cpath"], "filepath": x["file"]})

        for x in container_to_delete:
            await self.queue.put({"method": "DELETE", "container_path": x["cpath"], "filepath": x["file"]})

    async def send_data(self, d):
        try:
            await self.send_patch(d["method"], d["container_path"], d["filepath"], d.get("force", False))
        except WatcherError:
            pass

    async def producer(self):
        iter_number = 0
        while True:
            LOG.debug("Call producer")
            await self.get_data(iter_number)
            asyncio.sleep(self.watcher_interval)
            iter_number += 1

    async def consumer(self):
        while True:
            data = await self.queue.get()
            await self.send_data(data)

    async def watch(self):
        producer = asyncio.get_event_loop().create_task(self.producer())
        consumers = [asyncio.get_event_loop().create_task(self.consumer()) for _ in range(self.watcher_nb_threads)]

        await asyncio.gather(producer, *consumers)
