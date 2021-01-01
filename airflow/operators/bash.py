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


from typing import Dict, Optional

from cached_property import cached_property

from airflow.exceptions import AirflowException
from airflow.hooks.bash import BashHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class BashOperator(BaseOperator):
    r"""
    Execute a Bash script, command or set of commands.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BashOperator`

    If BaseOperator.do_xcom_push is True, the last line written to stdout
    will also be pushed to an XCom when the bash command completes

    :param bash_command: The command, set of commands or reference to a
        bash script (must be '.sh') to be executed. (templated)
    :type bash_command: str
    :param env: If env is not None, it must be a dict that defines the
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

    .. note::

        Add a space after the script name when directly calling a ``.sh`` script with the
        ``bash_command`` argument -- for example ``bash_command="my_script.sh "``.  This
        is because Airflow tries to apply load this file and process it as a Jinja template to
        it ends with ``.sh``, which will likely not be what most users want.

    .. warning::

        Care should be taken with "user" input or when using Jinja templates in the
        ``bash_command``, as this bash operator does not perform any escaping or
        sanitization of the command.

        This applies mostly to using "dag_run" conf, as that can be submitted via
        users in the Web UI. Most of the default template variables are not at
        risk.

    For example, do **not** do this:

    .. code-block:: python

        bash_task = BashOperator(
            task_id="bash_task",
            bash_command='echo "Here is the message: \'{{ dag_run.conf["message"] if dag_run else "" }}\'"',
        )

    Instead, you should pass this via the ``env`` kwarg and use double-quotes
    inside the bash_command, as below:

    .. code-block:: python

        bash_task = BashOperator(
            task_id="bash_task",
            bash_command='echo "here is the message: \'$message\'"',
            env={'message': '{{ dag_run.conf["message"] if dag_run else "" }}'},
        )

    """

    template_fields = ('bash_command', 'env')
    template_fields_renderers = {'bash_command': 'bash', 'env': 'json'}
    template_ext = (
        '.sh',
        '.bash',
    )
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
        self,
        *,
        bash_command: str,
        env: Optional[Dict[str, str]] = None,
        output_encoding: str = 'utf-8',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bash_command = bash_command
        self.env = env
        self.output_encoding = output_encoding
        if kwargs.get('xcom_push') is not None:
            raise AirflowException("'xcom_push' was deprecated, use 'BaseOperator.do_xcom_push' instead")
        self.sub_process = None

    @cached_property
    def bash_hook(self):
        """Returns hook for running bash commands"""
        return BashHook()

    def execute(self, context=None):
        return self.bash_hook.run_command(
            command=self.bash_command,
            context=context,
            env=self.env,
            output_encoding=self.output_encoding,
        )

    def on_kill(self) -> None:
        self.bash_hook.send_sigterm()
