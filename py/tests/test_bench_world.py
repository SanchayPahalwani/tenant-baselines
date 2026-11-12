import json
import math
import subprocess

import pytest

from tenant_baselines import bench_world, eval_report, freeze
from tenant_baselines.bench_fill import LATENCY_FACTOR
from tenant_baselines.traffic import LATENCY_SIGMA
from tenant_baselines.world import build_world


def test_bench_world_has_two_thousand_keys():
    w = bench_world.build("bench", 1)
    assert len({k.key for k in w.keys}) == len(w.keys) == 2000
    assert len(w.tenants) == 1000
    assert len(w.units) == 24
    units = {u.name: u for u in w.units}
    for t in w.tenants:
        mine = [k for k in w.keys if k.tenant == t.name]
        assert len(mine) == 2
        assert mine[0].unit != mine[1].unit
        assert {k.region for k in mine} == {t.home_region}
        assert all(units[k.unit].region == t.home_region for k in mine)
        assert t.steady and t.peak_rpm == 240.0
    by_region = {r.name: sum(k.region == r.name for k in w.keys) for r in w.regions}
    assert list(by_region.values()) == [668, 666, 666]
    assert w == bench_world.build("bench", 1)
    assert [k.tenant for k in w.keys[:4]] == ["b0001", "b0001", "b0002", "b0002"]
    assert [k.unit for k in w.keys[:2]] == ["us-east-s1", "us-east-s2"]


def test_unknown_bench_world_is_refused():
    with pytest.raises(ValueError, match="unknown bench world"):
        bench_world.build("demo", 1)


def test_bench_latencies_stay_under_the_static_threshold():
    sets = json.loads((freeze.REPO / "infra/flink/thresholds-demo.json").read_text())["sets"]
    fire = min(s["global"]["latency_p95"]["fire"] for s in sets)
    for seed in range(1, 6):
        w = bench_world.build("bench", seed)
        top = max(u.base_latency_ms for u in w.units)
        assert top * LATENCY_FACTOR * math.exp(1.645 * LATENCY_SIGMA) < fire


def test_measured_paths_equal_the_freeze():
    """A pin: the sealed eval paths are byte-identical to the freeze commit."""
    commit = json.loads((freeze.REPO / "eval/freeze.json").read_text())["commit"]
    try:
        known = subprocess.run(
            ["git", "cat-file", "-e", f"{commit}^{{commit}}"], cwd=freeze.REPO, capture_output=True
        )
    except FileNotFoundError:
        pytest.skip("git is not available")
    if known.returncode != 0:
        pytest.skip("the freeze commit is not available (no git history)")
    assert eval_report.display_only(eval_report.measured_code_diff(commit))


def test_eval_demo_and_paths_worlds_are_unchanged():
    """A pin: it passes before the bench world exists."""
    assert len(build_world("eval", 7).keys) == 48
    assert len(build_world("demo", 1).keys) == 22
    assert len(build_world("paths", 7).keys) == 10


def test_delay_world_has_twelve_tenants_a_region():
    from tenant_baselines.gen_live import LiveGenerator

    w = bench_world.build("delay", 1)
    assert len(w.keys) == len({k.key for k in w.keys}) == 36
    assert len(w.tenants) == 36
    assert [t.name for t in w.tenants][:2] == ["d01", "d02"]
    for r in w.regions:
        assert sum(k.region == r.name for k in w.keys) == 12
    units = {u.name: u.base_latency_ms for u in w.units}
    assert len(units) == 6
    assert sorted(units.values()) == [70.0] * 3 + [80.0] * 3
    for i, t in enumerate(w.tenants):
        mine = [k for k in w.keys if k.tenant == t.name]
        assert len(mine) == 1
        assert t.steady and t.peak_rpm == 40.0
        assert t.home_region == w.regions[i % 3].name == mine[0].region
        suffix = "-s1" if (i // 3) % 2 == 0 else "-s2"
        assert mine[0].unit == f"{t.home_region}{suffix}"
    assert w == bench_world.build("delay", 99)
    gen = LiveGenerator(w, 1, [])
    assert {gen.partition_of(k.key, 8) for k in w.keys} == set(range(8))


def test_delay_latencies_stay_under_the_static_threshold_at_40_a_minute():
    """A minute of 40 events has a p95 near base x 1.6 x exp(1.645 sigma) x 1.1; it must clear
    the demo file's static level with room, or static pairs page on latency in a delay run."""
    sets = json.loads((freeze.REPO / "infra/flink/thresholds-demo.json").read_text())["sets"]
    fire = min(s["global"]["latency_p95"]["fire"] for s in sets)
    top = max(u.base_latency_ms for u in bench_world.build("delay", 1).units)
    assert top * LATENCY_FACTOR * math.exp(1.645 * LATENCY_SIGMA) * 1.1 < fire
