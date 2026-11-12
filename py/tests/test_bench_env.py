import json
import subprocess
import time

import pytest
from confluent_kafka import TIMESTAMP_CREATE_TIME, TIMESTAMP_LOG_APPEND_TIME

from tenant_baselines import bench_env, kafka_io
from tenant_baselines.bench_env import NotAppendTime, PreflightFailed

AC = "Now drawing from 'AC Power'\n -InternalBattery-0 (id=1)\t100%; charged\n"
BATTERY = "Now drawing from 'Battery Power'\n -InternalBattery-0 (id=1)\t80%; discharging\n"


def done(out: str = "", rc: int = 0) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess([], rc, stdout=out, stderr="")


class Resp:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        return self

    def json(self):
        return self.body


def getter(routes: dict):
    def get(url, timeout=None):
        path = url.removeprefix("http://127.0.0.1:8081")
        return Resp(routes[path])

    return get


class Shell:
    """A fake `run`: the answer for a command is looked up by its first words."""

    def __init__(self, power=AC, vm_date=None, status="", running="", ours="", fail=None):
        self.power, self.status, self.running, self.ours, self.fail = (
            power,
            status,
            running,
            ours,
            fail,
        )
        self.vm_date = str(int(time.time())) if vm_date is None else vm_date
        self.calls: list[list[str]] = []

    def __call__(self, cmd):
        self.calls.append(cmd)
        if self.fail and cmd[: len(self.fail)] == self.fail:
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="boom")
        if cmd[0] == "pmset":
            return done(self.power)
        if cmd[:2] == ["git", "status"]:
            return done(self.status)
        if cmd[:3] == ["docker", "compose", "exec"]:
            return done(self.vm_date)
        if cmd[:2] == ["docker", "info"]:
            return done(
                json.dumps({"NCPU": 8, "MemTotal": 10 * 1024**3, "ServerVersion": "28.0.1"})
            )
        if cmd[:3] == ["docker", "ps", "-q"]:
            # like docker: 12-character ids unless asked for the full ones
            ids = self.running.split()
            return done("\n".join(i if "--no-trunc" in cmd else i[:12] for i in ids))
        if cmd[:4] == ["docker", "compose", "ps", "-q"]:
            return done(self.ours)
        if cmd[:3] == ["docker", "compose", "config"]:
            return done("b:2\na:1\n")
        if cmd[0] == "sysctl":
            return done(
                {
                    "machdep.cpu.brand_string": "Test CPU",
                    "hw.ncpu": "10",
                    "hw.memsize": str(16 * 1024**3),
                }[cmd[2]]
            )
        if cmd[0] == "sw_vers":
            return done("15.0")
        if cmd[0] == "uname":
            return done("arm64")
        raise AssertionError(cmd)


@pytest.fixture
def jar(tmp_path, monkeypatch):
    path = tmp_path / "job.jar"
    path.write_text("x")
    monkeypatch.setattr(bench_env, "JAR", path)
    return path


def overview(*states):
    return {"/jobs/overview": {"jobs": [{"name": f"job-{s}", "state": s} for s in states]}}


def test_preflight_refuses_battery(jar):
    get = getter(overview("FINISHED"))
    with pytest.raises(PreflightFailed, match="on battery"):
        bench_env.preflight(run=Shell(power=BATTERY), get=get)
    bench_env.preflight(on_battery=True, run=Shell(power=BATTERY), get=get)


def test_preflight_refuses_a_running_job(jar):
    running = {"/jobs/overview": {"jobs": [{"name": "tenant-baselines-live", "state": "RUNNING"}]}}
    with pytest.raises(PreflightFailed, match="tenant-baselines-live.*cancel it"):
        bench_env.preflight(run=Shell(), get=getter(running))
    bench_env.preflight(run=Shell(), get=getter(overview("FINISHED", "CANCELED")))


def test_preflight_refuses_any_job_that_is_not_over(jar):
    for state in ("RUNNING", "RECONCILING", "SUSPENDED", "CANCELLING", "FAILING", "CREATED"):
        with pytest.raises(PreflightFailed, match=state):
            bench_env.preflight(run=Shell(), get=getter(overview(state)))
    bench_env.preflight(run=Shell(), get=getter(overview("FINISHED", "CANCELED", "FAILED")))


def test_preflight_refuses_a_missing_jar(jar):
    jar.unlink()
    with pytest.raises(PreflightFailed, match="make jar"):
        bench_env.preflight(run=Shell(), get=getter(overview()))


def test_preflight_refuses_a_skewed_vm_clock(jar):
    with pytest.raises(PreflightFailed, match="clock"):
        bench_env.preflight(run=Shell(vm_date="1000"), get=getter(overview()))


FLINK = {
    "/config": {"flink-version": "2.2.1"},
    "/jobmanager/config": [
        {"key": "parallelism.default", "value": "2"},
        {"key": "taskmanager.numberOfTaskSlots", "value": "2"},
        {"key": "taskmanager.memory.process.size", "value": "3g"},
        {"key": "rest.port", "value": "8081"},
    ],
}


def cid(name: str) -> str:
    return (name * 64)[:64]


def test_collect_counts_other_containers_without_names(monkeypatch):
    monkeypatch.setattr(bench_env, "head_commit", lambda: "abc")
    got = bench_env.collect(
        False,
        run=Shell(running=" ".join(map(cid, "AUBVC")), ours=" ".join(map(cid, "UV"))),
        get=getter(FLINK),
    )
    assert got["other_containers"] == 3  # foreign ones first, mid-list and last
    assert cid("A") not in json.dumps(got)
    assert set(got) == {
        "data", "date_utc", "commit", "dirty", "on_battery", "host", "power", "vm",
        "other_containers", "images", "flink",
    }  # fmt: skip
    assert got["data"] == "synthetic" and got["images"] == ["a:1", "b:2"]
    assert got["host"] == {
        "cpu": "Test CPU", "cores": 10, "memory_gb": 16.0, "os": "15.0", "arch": "arm64",
    }  # fmt: skip
    assert got["vm"] == {"cpus": 8, "memory_gb": 10.0, "server_version": "28.0.1"}
    assert got["power"] == "Now drawing from 'AC Power'"
    assert got["flink"] == {
        "version": "2.2.1",
        "parallelism.default": "2",
        "taskmanager.numberOfTaskSlots": "2",
        "taskmanager.memory.process.size": "3g",
    }


def test_collect_counts_a_lone_foreign_container_at_either_end(monkeypatch):
    monkeypatch.setattr(bench_env, "head_commit", lambda: "abc")
    for running in (f"{cid('X')} {cid('U')}", f"{cid('U')} {cid('X')}"):
        got = bench_env.collect(False, run=Shell(running=running, ours=cid("U")), get=getter(FLINK))
        assert got["other_containers"] == 1


def test_collect_raises_when_a_command_fails(monkeypatch):
    monkeypatch.setattr(bench_env, "head_commit", lambda: "abc")
    with pytest.raises(RuntimeError, match=r"git status.*boom"):
        bench_env.collect(False, run=Shell(fail=["git", "status"]), get=getter(FLINK))


def test_collect_dirty_ignores_the_result_files(monkeypatch):
    monkeypatch.setattr(bench_env, "head_commit", lambda: "abc")
    clean = " M docs/performance/throughput.json\0?? docs/performance.md\0"
    assert not bench_env.collect(False, run=Shell(status=clean), get=getter(FLINK))["dirty"]
    assert bench_env.collect(False, run=Shell(status=clean + " M py/x.py\0"), get=getter(FLINK))[
        "dirty"
    ]
    stray = " M docs/performance.md.orig\0"
    assert bench_env.collect(False, run=Shell(status=stray), get=getter(FLINK))["dirty"]


def test_clock_offset_takes_the_tightest_bracket():
    host = iter([100.0, 100.4, 101.0, 101.1, 102.0, 102.6])
    vm = iter(["100.900000000", "101.750000000", "103.500000000"])
    got = bench_env.vm_clock_offset(run=lambda cmd: done(next(vm)), now=lambda: next(host), tries=3)
    assert got["offset_s"] == pytest.approx(0.70)
    assert got["bracket_s"] == pytest.approx(0.1)


def test_clock_offset_picks_the_try_not_the_first_or_last():
    host = iter([100.0, 100.4, 101.0, 101.1, 102.0, 102.6])
    vm = iter(["100.000000000", "101.850000000", "105.000000000"])
    got = bench_env.vm_clock_offset(run=lambda cmd: done(next(vm)), now=lambda: next(host), tries=3)
    assert got["offset_s"] == pytest.approx(0.80)  # the middle try: 101.85 - 101.05


@pytest.mark.parametrize("bad", ["1731400000.", "1731400000.N", "1731400000", "", "x.123456789"])
def test_clock_read_failure_names_the_output(bad):
    with pytest.raises(bench_env.ClockReadFailed, match="jobmanager's clock") as e:
        bench_env.vm_clock_offset(run=lambda cmd: done(bad), now=lambda: 1.0, tries=1)
    assert isinstance(e.value, PreflightFailed) and repr(bad) in str(e.value)


def test_gaps_finds_a_host_sleep():
    assert bench_env.gaps([0, 12, 24, 300, 312]) == [276]
    assert bench_env.gaps([0, 60, 120]) == []


def test_validity_lists_every_reason():
    ok, reasons = bench_env.validity(1, 3, [276.0], ["the stack was busy"])
    assert not ok and len(reasons) == 4
    assert "the stack was busy" in reasons
    assert bench_env.validity(0, 0, []) == (True, [])


class Msg:
    def __init__(self, partition, offset, kind, ts, value):
        self._a = (partition, offset, (kind, ts), json.dumps(value).encode())

    def error(self):
        return None

    def partition(self):
        return self._a[0]

    def offset(self):
        return self._a[1]

    def timestamp(self):
        return self._a[2]

    def value(self):
        return self._a[3]


class Consumer:
    def __init__(self, msgs, low=0):
        self.msgs, self.low = list(msgs), low

    def get_watermark_offsets(self, tp, timeout=None):
        return self.low, 100

    def assign(self, tps):
        self.assigned = tps

    def consume(self, num_messages, timeout):
        batch, self.msgs = self.msgs, []
        return batch


def test_read_stamped_returns_append_times():
    kind = TIMESTAMP_LOG_APPEND_TIME
    c = Consumer(
        [
            Msg(1, 5, kind, 2000, {"n": 3}),
            Msg(0, 0, kind, 1000, {"n": 1}),
            Msg(0, 1, kind, 1500, {"n": 2}),
            Msg(0, 2, kind, 9999, {"n": "past the end"}),
        ]
    )
    got = bench_env.read_stamped("t", {0: 0, 1: 5}, {0: 2, 1: 6}, consumer=c)
    assert got == [(1000, {"n": 1}), (1500, {"n": 2}), (2000, {"n": 3})]


def test_read_stamped_rejects_create_time():
    c = Consumer([Msg(0, 0, TIMESTAMP_CREATE_TIME, 1000, {})])
    with pytest.raises(NotAppendTime, match=r"bench\.in.*make reset"):
        bench_env.read_stamped("bench.in", {0: 0}, {0: 1}, consumer=c)


def test_read_stamped_rejects_a_gap():
    kind = TIMESTAMP_LOG_APPEND_TIME
    with pytest.raises(kafka_io.IncompleteRead, match="offset 2 where 1"):
        bench_env.read_stamped(
            "t",
            {0: 0},
            {0: 3},
            consumer=Consumer([Msg(0, 0, kind, 1, {}), Msg(0, 2, kind, 2, {})]),
        )


def test_read_stamped_rejects_a_short_read(monkeypatch):
    monkeypatch.setattr(kafka_io, "READ_IDLE_S", 0)
    kind = TIMESTAMP_LOG_APPEND_TIME
    with pytest.raises(kafka_io.IncompleteRead, match="read 1 records of 2"):
        bench_env.read_stamped("t", {0: 0}, {0: 2}, consumer=Consumer([Msg(0, 0, kind, 1, {})]))


def test_write_result_skips_the_docs_file_for_a_pilot(tmp_path, monkeypatch):
    monkeypatch.setattr(bench_env, "PERF_DIR", tmp_path / "perf")
    path = bench_env.write_result("throughput", {"b": 1, "a": [2]}, tmp_path / "run", pilot=True)
    assert path == tmp_path / "run" / "result.json"
    assert json.loads(path.read_text()) == {"a": [2], "b": 1}
    assert path.read_text().index('"a"') < path.read_text().index('"b"')
    assert not (tmp_path / "perf").exists()
    bench_env.write_result("throughput", {"a": 1}, tmp_path / "run2", pilot=False)
    assert json.loads((tmp_path / "perf" / "throughput.json").read_text()) == {"a": 1}


def test_write_result_refuses_nan(tmp_path):
    with pytest.raises(ValueError):
        bench_env.write_result("x", {"a": float("nan")}, tmp_path, pilot=True)


def fake_run(outputs: dict):
    def run(cmd):
        key = " ".join(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout=outputs[key], stderr="")

    return run


def test_load_record_splits_this_projects_cpu_from_the_others():
    ours, other1, other2 = "a" * 64, "b" * 64, "c" * 64
    run = fake_run(
        {
            "sysctl -n vm.loadavg": "{ 3.52 2.10 1.90 }\n",
            "docker compose ps -q": f"{ours}\n",
            "docker stats --no-stream --no-trunc --format {{.ID}} {{.CPUPerc}}": (
                f"{ours} 150.50%\n{other1} 20.25%\n{other2[:12]} 0.25%\n"
            ),
        }
    )
    got = bench_env.load_record(run)
    assert got == {
        "host_load_1m": 3.52,
        "project_containers_cpu_pct": 150.5,
        "other_containers_cpu_pct": 20.5,
    }


def test_load_record_raises_on_a_failed_command():
    def run(cmd):
        return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="boom")

    with pytest.raises(RuntimeError, match="boom"):
        bench_env.load_record(run)
