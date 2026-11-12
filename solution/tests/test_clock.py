from datetime import datetime, timedelta, timezone

import pytest

from dispatcher.clock import IST, SimClock

SIM_EPOCH = "2026-11-12T17:30:00+05:30"
REAL_EPOCH = 1_800_000_000.0  # any fixed unix time
SPEED = 600.0


def real(seconds_after_epoch: float) -> datetime:
    return datetime.fromtimestamp(REAL_EPOCH + seconds_after_epoch, tz=timezone.utc)


@pytest.fixture
def clock() -> SimClock:
    return SimClock(sim_epoch_iso=SIM_EPOCH, real_epoch=REAL_EPOCH, speed=SPEED)


def test_sim_time_at_epoch_is_sim_epoch(clock):
    assert clock.sim_at(real(0)) == datetime.fromisoformat(SIM_EPOCH)


def test_one_real_second_is_ten_sim_minutes(clock):
    assert clock.sim_at(real(1)) == datetime(2026, 11, 12, 17, 40, tzinfo=IST)


def test_sim_time_is_returned_in_ist(clock):
    assert clock.sim_at(real(6)).utcoffset() == timedelta(hours=5, minutes=30)


def test_real_seconds_for_sim_wait(clock):
    # 60 simulated minutes at 600x is 6 real seconds
    assert clock.real_seconds(60 * 60) == 6.0


def test_real_delay_until_future_sim_time(clock):
    target = datetime(2026, 11, 12, 18, 30, tzinfo=IST)  # 60 sim minutes after epoch
    assert clock.real_delay_until(target, real(0)) == pytest.approx(6.0)


def test_real_delay_until_past_time_is_zero(clock):
    target = datetime(2026, 11, 12, 17, 0, tzinfo=IST)
    assert clock.real_delay_until(target, real(0)) == 0.0


def test_works_with_utc_input_target(clock):
    target = datetime(2026, 11, 12, 13, 0, tzinfo=timezone.utc)  # 18:30 IST
    assert clock.real_delay_until(target, real(0)) == pytest.approx(6.0)


def test_rejects_naive_real_now(clock):
    with pytest.raises(ValueError):
        clock.sim_at(datetime(2026, 11, 12, 12, 0))


def test_rejects_bad_speed():
    with pytest.raises(ValueError):
        SimClock(sim_epoch_iso=SIM_EPOCH, real_epoch=REAL_EPOCH, speed=0)


def test_rejects_epoch_without_offset():
    with pytest.raises(ValueError):
        SimClock(sim_epoch_iso="2026-11-12T17:30:00", real_epoch=REAL_EPOCH, speed=SPEED)
