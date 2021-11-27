# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import json
import warnings
from abc import ABC
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from airflow.models.connection import Connection


class BaseSecretsBackend(ABC):
    """Abstract base class to retrieve Connection object given a conn_id or Variable given a key"""

    __NO_DEFAULT_SENTINEL = object()

    def __init__(self, connections_as_json: bool = False, **kwargs):
        self.connections_as_json = connections_as_json

    @staticmethod
    def build_path(path_prefix: str, secret_id: str, sep: str = "/") -> str:
        """
        Given conn_id, build path for Secrets Backend

        :param path_prefix: Prefix of the path to get secret
        :type path_prefix: str
        :param secret_id: Secret id
        :type secret_id: str
        :param sep: separator used to concatenate connections_prefix and conn_id. Default: "/"
        :type sep: str
        """
        return f"{path_prefix}{sep}{secret_id}"

    def get_conn_uri(self, conn_id: str) -> Optional[str]:
        """
        Get conn_uri from Secrets Backend

        This method is deprecated and will be removed in a future release; implement ``get_conn_value`` instead.

        :param conn_id: connection id
        :type conn_id: str
        """
        raise NotImplementedError()

    def get_conn_value(self, conn_id: str) -> Optional[str]:
        raise NotImplementedError

    def _build_conn_from_value(self, conn_id, value):
        from airflow.models.connection import Connection

        if self.connections_as_json:
            kwargs = json.loads(value)
            extra = kwargs.pop('extra', None)
            if extra:
                kwargs['extra'] = extra if isinstance(extra, str) else json.dumps(extra)
            return Connection(conn_id=conn_id, **kwargs)
        else:
            return Connection(conn_id=conn_id, uri=value)

    def get_connection(self, conn_id: str) -> Optional['Connection']:
        """
        Return connection object with a given ``conn_id``.

        Tries ``get_conn_value`` first and if not implement, tries ``get_conn_

        :param conn_id: connection id
        :type conn_id: str
        """

        conn_value = None

        # TODO: after removal of ``get_conn_uri`` we should not catch NotImplementedError here
        try:
            conn_value = self.get_conn_value(conn_id=conn_id)
        except NotImplementedError:
            try:
                conn_value = self.get_conn_uri(conn_id=conn_id)
            except NotImplementedError:  # we only catch here to allow use of only ``get_conn_value``
                pass

        # if get_conn_value was implemented, we should use it;
        # if conn_value is self.__NO_DEFAULT_SENTINEL:

        if conn_value and conn_value is not self.__NO_DEFAULT_SENTINEL:
            return self._build_conn_from_value(conn_id=conn_id, value=conn_value)
        else:
            return None

    def get_connections(self, conn_id: str) -> List['Connection']:
        """
        Return connection object with a given ``conn_id``.

        :param conn_id: connection id
        :type conn_id: str
        """
        warnings.warn(
            "This method is deprecated. Please use "
            "`airflow.secrets.base_secrets.BaseSecretsBackend.get_connection`.",
            PendingDeprecationWarning,
            stacklevel=2,
        )
        conn = self.get_connection(conn_id=conn_id)
        if conn:
            return [conn]
        return []

    def get_variable(self, key: str) -> Optional[str]:
        """
        Return value for Airflow Variable

        :param key: Variable Key
        :type key: str
        :return: Variable Value
        """
        raise NotImplementedError()

    def get_config(self, key: str) -> Optional[str]:
        """
        Return value for Airflow Config Key

        :param key: Config Key
        :return: Config Value
        """
        return None
