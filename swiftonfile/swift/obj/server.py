# Copyright (c) 2012-2014 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Object Server for Gluster for Swift """

import os
from eventlet import Timeout

from swift import gettext_ as _
from swift.common.swob import (
    HTTPConflict,
    HTTPNotImplemented,
    HTTPBadRequest,
    HTTPOk,
    HTTPNotFound,
    Response,
    HTTPServerError,
)
from swift.common.utils import (
    public,
    timing_stats,
    replication,
    config_true_value,
    Timestamp,
)
from swift.common.request_helpers import get_name_and_placement, split_and_validate_path
from swift.common.bufferedhttp import http_connect
from swiftonfile.swift.common.exceptions import AlreadyExistsAsFile, AlreadyExistsAsDir
from swift.common.header_key_dict import HeaderKeyDict
from swift.obj import server
from swift.common.ring import Ring
from swift.common.exceptions import (
    DiskFileNotExist,
    ConnectionTimeout,
    InvalidAccountInfo,
)

from swiftonfile.swift.obj.diskfile import DiskFileManager
from swiftonfile.swift.common.constraints import check_object_creation
from swiftonfile.swift.common import utils


class SwiftOnFileDiskFileRouter:
    """
    Replacement for Swift's DiskFileRouter object.
    Always returns SwiftOnFile's DiskFileManager implementation.
    """

    def __init__(self, *args, **kwargs):
        self.manager_cls = DiskFileManager(*args, **kwargs)

    def __getitem__(self, policy):
        return self.manager_cls


class ObjectController(server.ObjectController):
    """
    Subclass of the object server's ObjectController which replaces the
    container_update method with one that is a no-op (information is simply
    stored on disk and already updated by virtue of performing the file system
    operations directly).
    """

    def setup(self, conf):
        """
        Implementation specific setup. This method is called at the very end
        by the constructor to allow a specific implementation to modify
        existing attributes or add its own attributes.

        :param conf: WSGI configuration parameter
        """
        # Replaces Swift's DiskFileRouter object reference with ours.
        self._diskfile_router = SwiftOnFileDiskFileRouter(conf, self.logger)
        # This conf option will be deprecated and eventualy removed in
        # future releases
        utils.read_pickled_metadata = config_true_value(
            conf.get("read_pickled_metadata", "no")
        )

        self.swift_dir = conf.get("swift_dir", "/etc/swift")

    def watcher_container_list(self, container_path, host, partition, contdevice, subfolder=None):
        """
        List all files in a container

        :param container_path: full path to the container
        :param host: host that the container is on
        :param partition: partition that the container is on
        :param contdevice: device name that the container is on
        :param subfolder: subfolder in the container, None if all the container must be listed
        """
        params = {'headers': {"user-agent": "object-server %s" % os.getpid()}}
        if subfolder:
            params['query_string'] = f'prefix={subfolder}'

        op = "GET"

        if all([host, partition, contdevice]):
            try:
                with ConnectionTimeout(self.conn_timeout):
                    ip, port = host.rsplit(":", 1)
                    conn = http_connect(
                        ip, port, contdevice, partition, op, container_path,
                        **params
                    )
                with Timeout(self.node_timeout):
                    response = conn.getresponse()

                return response
            except (Exception, Timeout):
                self.logger.exception(
                    _("ERROR container list failed with " "%(ip)s:%(port)s/%(dev)s"),
                    {"ip": ip, "port": port, "dev": contdevice},
                )
                raise

        raise Exception(
            "Missing value: host={}, partition={}, condevice={}".format(
                host, partition, contdevice
            )
        )

    def watcher_container_update(
        self, op, container_path, obj, host, partition, contdevice, headers_out
    ):
        """
        Sends a sync update.

        :param op: operation performed (ex: 'PUT', or 'DELETE')
        :param container_path: full path to container
        :param obj: object name
        :param host: host that the container is on
        :param partition: partition that the container is on, must be str
        :param contdevice: device name that the container is on
        :param headers_out: dictionary of headers to send in the container
                            request
        """
        headers_out["user-agent"] = "object-server %s" % os.getpid()
        full_path = "/%s/%s" % (container_path, obj)

        if all([host, partition, contdevice]):
            try:
                with ConnectionTimeout(self.conn_timeout):
                    ip, port = host.rsplit(":", 1)
                    conn = http_connect(
                        ip, port, contdevice, partition, op, full_path, headers_out
                    )
                with Timeout(self.node_timeout):
                    response = conn.getresponse()

                return response
            except (Exception, Timeout):
                self.logger.exception(
                    _("ERROR container update failed with " "%(ip)s:%(port)s/%(dev)s"),
                    {"ip": ip, "port": port, "dev": contdevice},
                )
                raise

        raise Exception(
            "Missing value: host={}, partition={}, condevice={}".format(
                host, partition, contdevice
            )
        )

    @public
    @timing_stats()
    def PUT(self, request):
        try:
            device, partition, account, container, obj, policy = get_name_and_placement(
                request, 5, 5, True
            )

            # check swiftonfile constraints first
            error_response = check_object_creation(request, obj)
            if error_response:
                return error_response

            # now call swift's PUT method
            return server.ObjectController.PUT(self, request)
        except (AlreadyExistsAsFile, AlreadyExistsAsDir):
            device = split_and_validate_path(request, 1, 5, True)
            return HTTPConflict(drive=device, request=request)
        except InvalidAccountInfo as e:
            return HTTPConflict(request=request, body=str(e))

    @public
    @timing_stats()
    def PATCH(self, request):
        """
        Must be called as PATCH http://ip:port/device/account/container/obj

        Example:
        curl
            -H "X-Patch-Method: DELETE" # or -H "X-Patch-Method: PUT"
            --request PATCH
            http://localhost:6200/lustre/AUTH_696257/container1/file.txt

        This method is used to let know to swifonfile that a file has
        been modified (crud) directly on the filesystem.
        This method update the file metadata and the container listing.

        To Get containers list:

        >>> from swift.common.ring import Ring
        >>> container_ring = Ring("/etc/swift", ring_name="container")
        >>> container_ring.get_nodes('AUTH_6962578c3977405cba5041b000e06abf',
            'container1')

        (1, [{'device': 'lustre', 'id': 0, 'ip': 'example.com', 'meta': '',
        'port': 6201, 'region': 1, 'replication_ip': 'example.com',
        'replication_port': 6201, 'weight': 1.0, 'zone': 1, 'index': 0}])
        The first entry in the tuple is the number of partition
        """
        device, account, container, obj, policy = get_name_and_placement(
            request, 4, 4, True
        )

        container_ring = Ring(self.swift_dir, ring_name="container")
        contpartition, updates = container_ring.get_nodes(account, container)

        disk_file = self.get_diskfile(
            device, contpartition, account, container, obj, policy=policy
        )

        try:
            method = request.headers["x-patch-method"]
            if method != "PUT" and method != "DELETE":
                raise Exception("X-Patch-Method must be PUT or DELETE")
        except Exception:
            return HTTPBadRequest(
                "You must pass X-Patch-Method Header with PUT or DELETE"
            )

        if method == "PUT":
            # Check file has no metadata
            # If file has metadata, it has been created with API
            # Moreover, this will ensure that file exists
            # We can force put with the header x-patch-force
            force_put = request.headers.get("x-patch-force", "False").lower() == 'true'
            try:
                if disk_file.has_metadata() and not force_put:
                    return HTTPBadRequest("You can't put an already registered file")
            except DiskFileNotExist:
                return HTTPBadRequest("You can't put an unexisting file")

            # Don't need to catch DiskFileNotExist again
            metadata = disk_file.read_metadata()
            update_headers = HeaderKeyDict(
                {
                    "x-size": metadata["Content-Length"],
                    "x-content-type": metadata["Content-Type"],
                    "x-timestamp": metadata["X-Timestamp"],
                    "x-etag": metadata["ETag"],
                }
            )
        else:
            # Check file not existing
            try:
                disk_file.read_metadata()
            except DiskFileNotExist:
                # It's ok, the file must not exist
                pass
            else:
                return HTTPBadRequest("You can't delete an existing file")

            update_headers = HeaderKeyDict({"x-timestamp": Timestamp.now().internal})

        contdevice = updates[0]["device"]
        conthost = ":".join([updates[0]["ip"], str(updates[0]["port"])])
        update_headers["x-trans-id"] = request.headers.get("x-trans-id", "-")
        update_headers["referer"] = request.as_referer()
        update_headers["X-Backend-Storage-Policy-Index"] = int(policy)

        container_path = "{}/{}".format(account, container)
        response = self.watcher_container_update(
            method,
            container_path,
            obj,
            conthost,
            str(contpartition),
            contdevice,
            update_headers,
        )

        if response.status in (200, 201, 202, 204):
            return HTTPOk()
        elif response.status == 400 or response.status == 404:
            return HTTPNotFound("Non existent container")

        return HTTPServerError("status error {}".format(response.status))

    @public
    @timing_stats()
    def LIST(self, request):
        """
        Must be called as LIST http://ip:port/device/account/container[/subfolder]

        Example:
        curl
            --request LIST
            http://localhost:6200/lustre/AUTH_696257/container1

        Example:
        curl
            --request LIST
            http://localhost:6200/lustre/AUTH_696257/container1/mysubfolder

        This method is used to get all files in a container or in a subfolder.
        """
        # subfolder is None if there is no subfolder
        device, account, container, subfolder = split_and_validate_path(request, 3, 4, True)

        container_path = "/{}/{}".format(account, container)

        container_ring = Ring(self.swift_dir, ring_name="container")
        contpartition, updates = container_ring.get_nodes(account, container)
        u = updates[0]  # only one container nodes (distributed by fs)

        host = ":".join([u["ip"], str(u["port"])])
        contdevice = u["device"]

        response = self.watcher_container_list(
            container_path, host, str(contpartition), contdevice, subfolder
        )

        if response.status == 200:
            return Response(body=response.read())
        elif response.status == 404:
            return HTTPNotFound("Non existent container")

        return HTTPServerError("status error {}".format(response.status))

    @public
    @replication
    @timing_stats(sample_rate=0.1)
    def REPLICATE(self, request):
        """
        In Swift, this method handles REPLICATE requests for the Swift
        Object Server.  This is used by the object replicator to get hashes
        for directories.

        Swiftonfile does not support this as it expects the underlying
        filesystem to take care of replication. Also, swiftonfile has no
        notion of hashes for directories.
        """
        return HTTPNotImplemented(request=request)

    @public
    @replication
    @timing_stats(sample_rate=0.1)
    def REPLICATION(self, request):
        return HTTPNotImplemented(request=request)


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI object server apps"""
    conf = global_conf.copy()
    conf.update(local_conf)
    return ObjectController(conf)
