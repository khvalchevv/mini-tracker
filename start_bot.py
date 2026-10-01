"""Start / stop / status for the bot as a DETACHED background process (Windows).

Why a launcher: starting through WMI (Win32_Process.Create) stopped working
after the 2026-10-01 reboot -- the cmd.exe children hang before running
anything -- and a console window invites an accidental close (the Sept 30
01:33 stop left no traceback and no system event; a closed window is the
most likely cause). A detached process has no window at all: tracebacks go
to logs/bot.err, everything else to logs/bot.log, the PID to logs/bot.pid.

    python start_bot.py            # start (refuses if already running)
    python start_bot.py stop       # kill the process named in logs/bot.pid
    python start_bot.py status
"""
import ctypes
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
LOGS = os.path.join(HERE, "logs")
PID_FILE = os.path.join(LOGS, "bot.pid")
PY = sys.executable

BREAKAWAY_FROM_JOB = 0x01000000
DETACHED_PROCESS = 0x00000008
NEW_PROCESS_GROUP = 0x00000200


def _alive(pid: int) -> bool:
    k = ctypes.windll.kernel32
    h = k.OpenProcess(0x1000, False, pid)          # PROCESS_QUERY_LIMITED_INFORMATION
    if not h:
        return False
    try:
        code = ctypes.c_ulong()
        return bool(k.GetExitCodeProcess(h, ctypes.byref(code))) and code.value == 259
    finally:
        k.CloseHandle(h)


def _is_our_main(pid: int) -> bool:
    try:
        out = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             f"(Get-CimInstance Win32_Process -Filter 'ProcessId={pid}').CommandLine"],
            capture_output=True, text=True, timeout=20).stdout
        return "main.py" in out
    except Exception:
        return True                                 # cannot tell -- trust the pid file


def _running_pid() -> int:
    try:
        pid = int(open(PID_FILE).read().strip() or 0)
    except (FileNotFoundError, ValueError):
        return 0
    return pid if pid and _alive(pid) and _is_our_main(pid) else 0


def start() -> int:
    pid = _running_pid()
    if pid:
        print(f"already running (pid {pid})")
        return 0
    os.makedirs(LOGS, exist_ok=True)
    err = open(os.path.join(LOGS, "bot.err"), "ab")
    common = dict(cwd=HERE, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                  stderr=err, close_fds=True)
    try:
        p = subprocess.Popen([PY, "main.py"], creationflags=BREAKAWAY_FROM_JOB | DETACHED_PROCESS | NEW_PROCESS_GROUP, **common)
    except OSError:                                 # job forbids breakaway -- stay inside it
        p = subprocess.Popen([PY, "main.py"], creationflags=DETACHED_PROCESS | NEW_PROCESS_GROUP, **common)
    with open(PID_FILE, "w") as f:
        f.write(str(p.pid))
    time.sleep(3)
    if p.poll() is not None:
        print(f"died at startup (rc {p.returncode}) -- see logs/bot.err")
        return 1
    print(f"started pid {p.pid} (detached, no window) -- logs/bot.log")
    return 0


def stop() -> int:
    pid = _running_pid()
    if not pid:
        print("not running")
        return 0
    subprocess.run(["taskkill", "/pid", str(pid), "/f"], capture_output=True)
    for _ in range(40):
        if not _alive(pid):
            break
        time.sleep(0.25)
    print(f"stopped pid {pid}" if not _alive(pid) else f"pid {pid} still alive")
    return 0


def status() -> int:
    pid = _running_pid()
    print(f"running (pid {pid})" if pid else "not running")
    return 0 if pid else 3


if __name__ == "__main__":
    cmd = (sys.argv[1] if len(sys.argv) > 1 else "start").lower()
    fn = {"start": start, "stop": stop, "status": status}.get(cmd)
    if fn is None:
        print(__doc__)
        sys.exit(2)
    sys.exit(fn())
