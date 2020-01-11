# -*- coding: utf-8 -*-
#
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


import os
from typing import Dict, Optional

from airflow.exceptions import AirflowException
from airflow.hooks.bash_hook import BashHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.operator_helpers import context_to_airflow_vars


class BashOperator(BaseOperator):
    """
    Execute a Bash script, command or set of commands.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BashOperator`

    If BaseOperator.do_xcom_push is True, the last line written to stdout
    will also be pushed to an XCom when the bash command completes

    :param bash_command: The command, set of commands or reference to a
        bash script (must be '.sh') to be executed. (templated)
    :type bash_command: str
    :param env: If env is not None, it must be a mapping that defines the
        environment variables for the new process; these are used instead
        of inheriting the current process environment, which is the default
        behavior. (templated)
    :type env: dict
    :param output_encoding: Output encoding of bash command
    :type output_encoding: str

    On execution of this operator the task will be up for retry
    when exception is raised. However, if a sub-command exits with non-zero
    value Airflow will not recognize it as failure unless the whole shell exits
    with a failure. The easiest way of achieving this is to prefix the command
    with ``set -e;``
    Example:

    .. code-block:: python

        bash_command = "set -e; python3 script.py '{{ next_execution_date }}'"
    """

    template_fields = ('bash_command', 'env')
    template_ext = (
        '.sh',
        '.bash',
    )
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
        self,
        bash_command: str,
        env: Optional[Dict[str, str]] = None,
        output_encoding: str = 'utf-8',
        *args,
        **kwargs
    ) -> None:

        super().__init__(*args, **kwargs)
        self.bash_command = bash_command
        self.output_encoding = output_encoding
        if kwargs.get('xcom_push') is not None:
            raise AirflowException(
                "'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead"
            )
        self.bash_hook = None
        self.env = env
        self.lineage_data = bash_command

    def get_env(self, context):
        env = self.env or os.environ.copy()

        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        self.log.info(
            'Exporting the following env vars:\n%s',
            '\n'.join(["{}={}".format(k, v) for k, v in airflow_context_vars.items()]),
        )
        env.update(airflow_context_vars)
        return env

    def execute(self, context):
        self.bash_hook = BashHook(output_encoding=self.output_encoding)

        line = self.bash_hook.run_command(
            bash_command=self.bash_command, env=self.get_env(context),
        )
        return line

    def on_kill(self):
        if hasattr(self.bash_hook, 'send_sigterm'):
            self.bash_hook.send_sigterm()
