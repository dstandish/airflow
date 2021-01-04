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

import unittest
from pathlib import Path
from subprocess import PIPE, STDOUT
from tempfile import TemporaryDirectory
from unittest import mock

from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.hooks.bash import BashHook

OS_ENV_KEY = 'BASH_ENV_TEST'
OS_ENV_VAL = 'this-is-from-os-environ'


class TestBashOperator(unittest.TestCase):
    @parameterized.expand(
        [
            ('with env', {'ABC': '123', 'AAA': '456'}, {'ABC': '123', 'AAA': '456', OS_ENV_KEY: ''}),
            ('no env', None, {OS_ENV_KEY: OS_ENV_VAL}),
        ]
    )
    def test_env(self, name, env, expected):
        """
        Test that env variables are exported correctly to the bash environment.
        When ``env`` is not provided, ``os.environ`` should be passed to ``Popen``.
        Otherwise, the variables in ``env`` should be available, and ``os.environ`` should not.
        """
        print(f"test_name: {name}")
        hook = BashHook()

        def build_cmd(keys, filename):
            """
            Produce bash command to echo env vars into filename.
            Will always echo the special test var named ``OS_ENV_KEY`` into the file to test whether
            ``os.environ`` is passed or not.
            """
            return '\n'.join([f"echo {k}=${k}>> {filename}" for k in [*keys, OS_ENV_KEY]])

        with TemporaryDirectory() as tmp_dir, mock.patch.dict('os.environ', {OS_ENV_KEY: OS_ENV_VAL}):
            tmp_file = Path(tmp_dir, 'test.txt')
            command = build_cmd(env and env.keys() or [], tmp_file.as_posix())
            hook.run_command(command=command, env=env)
            actual = dict([x.split('=') for x in tmp_file.read_text().splitlines()])
            assert actual == expected

    def test_return_value(self):
        hook = BashHook()
        return_value = hook.run_command(command='echo "stdout"')
        self.assertEqual(return_value, 'stdout')

    def test_raise_exception_on_non_zero_exit_code(self):
        hook = BashHook()
        with pytest.raises(
            AirflowException, match="Bash command failed\\. The command returned a non-zero exit code\\."
        ):
            hook.run_command(command='exit 42')

    @mock.patch.dict('os.environ', clear=True)
    @mock.patch(
        "airflow.hooks.bash.TemporaryDirectory",
        **{'return_value.__enter__.return_value': '/tmp/airflowtmpcatcat'},  # type: ignore
    )
    @mock.patch(
        "airflow.hooks.bash.Popen",
        **{  # type: ignore
            'return_value.stdout.readline.side_effect': [b'BAR', b'BAZ'],
            'return_value.returncode': 0,
        },
    )
    def test_should_exec_subprocess(self, mock_popen, mock_temporary_directory):
        hook = BashHook()
        hook.run_command(command='echo "stdout"')

        mock_popen.assert_called_once_with(
            ['bash', '-c', 'echo "stdout"'],
            cwd='/tmp/airflowtmpcatcat',
            env={},
            preexec_fn=mock.ANY,
            stderr=STDOUT,
            stdout=PIPE,
        )
