import pytest

from testgres import ExecUtilException
from testgres import LocalOperations


class TestLocalOperations:

    @pytest.fixture(scope="function", autouse=True)
    def setup(self):
        self.operations = LocalOperations()

    def test_exec_command_success(self):
        """
        Test exec_command for successful command execution.
        """
        cmd = "python3 --version"
        response = self.operations.exec_command(cmd, wait_exit=True, shell=True)

        assert b'Python 3.' in response

    def test_exec_command_failure(self):
        """
        Test exec_command for command execution failure.
        """
        cmd = "nonexistent_command"
        while True:
            try:
                self.operations.exec_command(cmd, wait_exit=True, shell=True)
            except ExecUtilException as e:
                error = e.message
                break
            raise Exception("We wait an exception!")
        assert error == "Utility exited with non-zero code. Error `b'/bin/sh: 1: nonexistent_command: not found\\n'`"

    def test_exec_command_failure__expect_error(self):
        """
        Test exec_command for command execution failure.
        """
        cmd = "nonexistent_command"

        exit_status, result, error = self.operations.exec_command(cmd, verbose=True, wait_exit=True, shell=True, expect_error=True)

        assert error == b'/bin/sh: 1: nonexistent_command: not found\n'
        assert exit_status == 127
        assert result == b''
