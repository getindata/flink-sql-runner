import subprocess


def run_cmd(cmd, throw_on_error=True, stream_output=False, **kwargs):
    print("CMD", cmd)
    if stream_output:
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception("Non-zero exitcode: %s" % exit_code)  # noqa: S001
        return exit_code
    else:
        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
        (stdout, stderr) = child.communicate()
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception(
                "Non-zero exitcode: %s\n\nSTDOUT:\n%s\n\nSTDERR:%s"  # noqa: S001
                % (exit_code, stdout.decode("utf-8"), stderr.decode("utf-8"))
            )
        return exit_code, stdout.decode("utf-8"), stderr.decode("utf-8")
