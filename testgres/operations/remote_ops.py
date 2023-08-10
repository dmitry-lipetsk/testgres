import os
import tempfile
import time
import subprocess
import sshtunnel
from ..exceptions import ExecUtilException
from .os_ops import OsOperations, ConnectionParams, pglib

sshtunnel.SSH_TIMEOUT = 5.0
sshtunnel.TUNNEL_TIMEOUT = 5.0

error_markers = ['error', 'Permission denied', 'fatal', 'No such file or directory']


class PsUtilProcessProxy:
    def __init__(self, host, username, ssh_key, pid):
        self.host = host
        self.username = username
        self.ssh_key = ssh_key
        self.pid = pid

    def _run_command(self, command):
        cmd = ["ssh", "-i", self.ssh_key, f"{self.username}@{self.host}", command]
        return subprocess.run(cmd, capture_output=True, text=True)

    def kill(self):
        command = "kill {}".format(self.pid)
        self._run_command(command)

    def cmdline(self):
        command = "ps -p {} -o cmd --no-headers".format(self.pid)
        result = self._run_command(command)
        return result.stdout.strip().split()


class RemoteOperations(OsOperations):
    def __init__(self, conn_params: ConnectionParams):
        super().__init__(conn_params.username)
        self.conn_params = conn_params
        self.host = conn_params.host
        self.ssh_key = conn_params.ssh_key
        self.remote = True
        self.username = conn_params.username or self.get_user()
        self.tunnel = None

    def __enter__(self):
        return self

    def __exit__(self):
        self.close_tunnel()

    def close_tunnel(self):
        if getattr(self, 'tunnel', None):
            self.tunnel.stop(force=True)
            start_time = time.time()
            while self.tunnel.is_active:
                if time.time() - start_time > sshtunnel.TUNNEL_TIMEOUT:
                    break
                time.sleep(0.5)

    def scp_upload(self, local_file: str, remote_file: str):
        """Upload a file to a remote host using scp."""
        command = [
            "scp",
            "-i", self.ssh_key,
            local_file,
            "{}@{}:{}".format(self.username, self.host, remote_file)
        ]
        subprocess.run(command, check=True)

    def ssh_command(self, cmd) -> list:
        """Prepare a command for SSH execution using subprocess."""
        if isinstance(cmd, list):
            base_cmd = [
                           "ssh",
                           "-i", self.ssh_key,
                           "{}@{}".format(self.username, self.host)
                       ] + cmd
        else:
            base_cmd = [
                "ssh",
                "-i", self.ssh_key,
                "{}@{}".format(self.username, self.host),
                cmd
            ]
        return base_cmd

    def exec_command(self, cmd: str, wait_exit=False, verbose=False, expect_error=False,
                     encoding='utf-8', shell=True, text=False, input=None, stdin=None, stdout=None,
                     stderr=None, proc=None):
        """
        Execute a command in the SSH session using subprocess.
        Args:
        - cmd (str): The command to be executed.
        - encoding (str|None): 'utf-8' is default. If encoding=None, then return binary
        """
        command = self.ssh_command(cmd)

        proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, shell=True)

        result, error = proc.communicate(input=input)

        exit_status = proc.returncode

        # Always decode result and error
        if encoding:
            result = result.decode(encoding)
            error = error.decode(encoding)

            error_found = exit_status != 0 or any(
                marker in error for marker in error_markers)
        else:
            error_found = exit_status != 0 or any(
                marker.encode() in error for marker in error_markers)

        if error_found:
            if exit_status == 0:
                exit_status = 1
            if expect_error:  # If we expect an error, return the error details instead of raising an exception
                return exit_status, result, error
            else:
                message = f"Utility exited with non-zero code. Error: {error}"
                raise ExecUtilException(message=message,
                                        command=cmd,
                                        exit_code=exit_status,
                                        out=result)

        if verbose:
            return exit_status, result, error
        else:
            return result

    # Environment setup
    def environ(self, var_name: str) -> str:
        """
        Get the value of an environment variable.
        Args:
        - var_name (str): The name of the environment variable.
        """
        cmd = "echo \${}".format(var_name)
        return self.exec_command(cmd).strip()

    def find_executable(self, executable):
        search_paths = self.environ("PATH")
        if not search_paths:
            return None

        search_paths = search_paths.split(self.pathsep)
        for path in search_paths:
            remote_file = os.path.join(path, executable)
            if self.isfile(remote_file):
                return remote_file

        return None

    def is_executable(self, file):
        # Check if the file is executable
        try:
            is_exec = self.exec_command("test -x {} && echo OK".format(file), expect_error=True)
            return is_exec == "OK\n"
        except ExecUtilException:
            return False

    def set_env(self, var_name: str, var_val: str):
        """
        Set the value of an environment variable.
        Args:
        - var_name (str): The name of the environment variable.
        - var_val (str): The value to be set for the environment variable.
        """
        return self.exec_command("export {}={}".format(var_name, var_val))

    # Get environment variables
    def get_user(self):
        return self.exec_command("echo \$USER").strip()

    def get_name(self):
        cmd = 'python3 -c "import os; print(os.name)"'
        return self.exec_command(cmd).strip()

    # Work with dirs
    def makedirs(self, path, remove_existing=False):
        """
        Create a directory in the remote server.
        Args:
        - path (str): The path to the directory to be created.
        - remove_existing (bool): If True, the existing directory at the path will be removed.
        """
        if remove_existing:
            cmd = "rm -rf {} && mkdir -p {}".format(path, path)
        else:
            cmd = "mkdir -p {}".format(path)
        try:
            exit_status, result, error = self.exec_command(cmd, verbose=True)
        except ExecUtilException as e:
            raise Exception("Couldn't create dir {} because of error {}".format(path, e.message))
        if exit_status != 0:
            raise Exception("Couldn't create dir {} because of error {}".format(path, error))
        return result

    def rmdirs(self, path, verbose=False, ignore_errors=True):
        """
        Remove a directory in the remote server.
        Args:
        - path (str): The path to the directory to be removed.
        - verbose (bool): If True, return exit status, result, and error.
        - ignore_errors (bool): If True, do not raise error if directory does not exist.
        """
        cmd = "rm -rf {}".format(path)
        exit_status, result, error = self.exec_command(cmd, verbose=True)
        if verbose:
            return exit_status, result, error
        else:
            return result

    def listdir(self, path):
        """
        List all files and directories in a directory.
        Args:
        path (str): The path to the directory.
        """
        result = self.exec_command("ls {}".format(path))
        return result.splitlines()

    def path_exists(self, path):
        result = self.exec_command("test -e {}; echo \$?".format(path))
        return int(result.strip()) == 0

    @property
    def pathsep(self):
        os_name = self.get_name()
        if os_name == "posix":
            pathsep = ":"
        elif os_name == "nt":
            pathsep = ";"
        else:
            raise Exception("Unsupported operating system: {}".format(os_name))
        return pathsep

    def mkdtemp(self, prefix=None):
        """
        Creates a temporary directory in the remote server.
        Args:
        - prefix (str): The prefix of the temporary directory name.
        """
        if prefix:
            temp_dir = self.exec_command("mktemp -d {}XXXXX".format(prefix))
        else:
            temp_dir = self.exec_command("mktemp -d")

        if temp_dir:
            if not os.path.isabs(temp_dir):
                temp_dir = os.path.join('/home', self.username, temp_dir.strip())
            return temp_dir
        else:
            raise ExecUtilException("Could not create temporary directory.")

    def mkstemp(self, prefix=None):
        if prefix:
            temp_dir = self.exec_command("mktemp {}XXXXX".format(prefix))
        else:
            temp_dir = self.exec_command("mktemp")

        if temp_dir:
            if not os.path.isabs(temp_dir):
                temp_dir = os.path.join('/home', self.username, temp_dir.strip())
            return temp_dir
        else:
            raise ExecUtilException("Could not create temporary directory.")

    def copytree(self, src, dst):
        if not os.path.isabs(dst):
            dst = os.path.join('~', dst)
        if self.isdir(dst):
            raise FileExistsError("Directory {} already exists.".format(dst))
        return self.exec_command("cp -r {} {}".format(src, dst))

    # Work with files
    def write(self, filename, data, truncate=False, binary=False, read_and_write=False, encoding='utf-8'):
        """
        Write data to a file on a remote host using scp and subprocess.
        """
        mode = "wb" if binary else "w"
        if not truncate:
            mode = "ab" if binary else "a"
        if read_and_write:
            mode = "r+b" if binary else "r+"

        with tempfile.NamedTemporaryFile(mode=mode, delete=False) as tmp_file:
            if not truncate:
                # Download the remote file first
                subprocess.run(self.ssh_command(f"cat {filename}"), stdout=tmp_file, check=False)
                tmp_file.seek(0, os.SEEK_END)

            if isinstance(data, bytes) and not binary:
                data = data.decode(encoding)
            elif isinstance(data, str) and binary:
                data = data.encode(encoding)

            if isinstance(data, list):
                # ensure each line ends with a newline
                data = [(s if isinstance(s, str) else s.decode('utf-8')).rstrip('\n') + '\n' for s in data]
                tmp_file.writelines(data)
            else:
                tmp_file.write(data)
            tmp_file.flush()

            # Upload the file back to the remote host
            self.scp_upload(tmp_file.name, filename)

            os.remove(tmp_file.name)

    def touch(self, filename):
        """
        Create a new file or update the access and modification times of an existing file on the remote server.

        Args:
            filename (str): The name of the file to touch.

        This method behaves as the 'touch' command in Unix. It's equivalent to calling 'touch filename' in the shell.
        """
        self.exec_command("touch {}".format(filename))

    def read(self, filename, encoding='utf-8'):
        cmd = "cat {}".format(filename)
        result = self.exec_command(cmd, encoding=encoding)
        return result

    def readlines(self, filename, num_lines=0, binary=False, encoding=None):
        if num_lines > 0:
            cmd = "tail -n {} {}".format(num_lines, filename)
        else:
            cmd = "cat {}".format(filename)

        result = self.exec_command(cmd, encoding=encoding)

        if not binary and result:
            lines = result.decode(encoding or 'utf-8').splitlines()
        else:
            lines = result.splitlines()

        return lines

    def isfile(self, remote_file):
        stdout = self.exec_command("test -f {}; echo \$?".format(remote_file))
        result = int(stdout.strip())
        return result == 0

    def isdir(self, dirname):
        cmd = "test -d {} && echo True || echo False".format(dirname)
        response = self.exec_command(cmd)
        return response.strip() == "True"

    def remove_file(self, filename):
        cmd = "rm {}".format(filename)
        return self.exec_command(cmd)

    # Processes control
    def kill(self, pid, signal):
        # Kill the process
        cmd = "kill -{} {}".format(signal, pid)
        return self.exec_command(cmd)

    def get_pid(self):
        # Get current process id
        return int(self.exec_command("echo \$$"))

    def get_process_children(self, pid):
        command = self.ssh_command(f"pgrep -P {pid}")

        result = subprocess.run(command, capture_output=True, text=True, check=True)

        children = result.stdout.splitlines()

        return [PsUtilProcessProxy(int(child_pid.strip())) for child_pid in children]

    # Database control
    def db_connect(self, dbname, user, password=None, host="127.0.0.1", port=5432, ssh_key=None):
        """
        Connects to a PostgreSQL database on the remote system.
        Args:
        - dbname (str): The name of the database to connect to.
        - user (str): The username for the database connection.
        - password (str, optional): The password for the database connection. Defaults to None.
        - host (str, optional): The IP address of the remote system. Defaults to "localhost".
        - port (int, optional): The port number of the PostgreSQL service. Defaults to 5432.

        This function establishes a connection to a PostgreSQL database on the remote system using the specified
        parameters. It returns a connection object that can be used to interact with the database.
        """
        self.close_tunnel()
        self.tunnel = sshtunnel.open_tunnel(
            (host, 22),  # Remote server IP and SSH port
            ssh_username=user or self.username,
            ssh_pkey=ssh_key or self.ssh_key,
            remote_bind_address=(host, port),  # PostgreSQL server IP and PostgreSQL port
            local_bind_address=('localhost', port)  # Local machine IP and available port
        )

        self.tunnel.start()

        try:
            conn = pglib.connect(
                host=host,  # change to 'localhost' because we're connecting through a local ssh tunnel
                port=self.tunnel.local_bind_port,  # use the local bind port set up by the tunnel
                database=dbname,
                user=user or self.username,
                password=password
            )

            return conn
        except Exception as e:
            self.tunnel.stop()
            raise ExecUtilException("Could not create db tunnel. {}".format(e))
