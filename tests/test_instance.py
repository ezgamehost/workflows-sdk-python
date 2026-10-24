import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

import ezgh_workflows._http as http
import ezgh_workflows._instance as inst
from ezgh_workflows import Client, NoBotError, QueuesClient

KEY1 = "ezgh_" + "a" * 40
KEY2 = "ezgh_" + "b" * 40


class Fake:
    """The metadata service and an API, as the _http.request they are reached through."""

    def __init__(self) -> None:
        self.now = 1_000_000.0
        self.puts = self.gets = 0
        self.token = ""
        self.keys = [KEY1]
        self.status = 0
        self.retry_after = ""
        self.ttl = 600.0
        self.refused: set[str] = set()
        self.api_auth: list[str] = []

    def __call__(
        self, method: str, url: str, headers: dict[str, str], body: bytes | None, timeout: float
    ) -> http.Response:
        if url.startswith(inst.METADATA_URL):
            if url.endswith("/token"):
                self.puts += 1
                self.token = f"tok{self.puts}"
                return http.Response(200, {}, self.token.encode())
            self.gets += 1
            if headers.get("EZGH-Metadata-Token") != self.token:
                return http.Response(401, {}, b'{"error":{"code":"invalid_token"}}')
            if self.status:
                return http.Response(self.status, {"retry-after": self.retry_after} if self.retry_after else {}, b"")
            k = self.keys.pop(0) if len(self.keys) > 1 else self.keys[0]
            exp = datetime.fromtimestamp(self.now + self.ttl, UTC).isoformat().replace("+00:00", "Z")
            return http.Response(200, {}, json.dumps({"apiKey": k, "expiresAt": exp, "bot": "bot_abc"}).encode())
        auth = headers["Authorization"]
        self.api_auth.append(auth)
        if auth in self.refused:
            return http.Response(401, {}, b'{"error":{"code":"unauthorized"}}')
        return http.Response(200, {}, b'{"messages":[{"messageId":"m"}]}')


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Fake:
    f = Fake()
    for mod in ("ezgh_workflows._instance", "ezgh_workflows.client", "ezgh_workflows.queues"):
        monkeypatch.setattr(f"{mod}.request", f)
    monkeypatch.setattr("ezgh_workflows.client._sleep", lambda s: None)
    p = tmp_path / "product_name"
    p.write_text("EZGH Compute\n")
    monkeypatch.setattr(inst, "PRODUCT_NAME_PATH", str(p))
    return f


def test_provider_recovers(fake: Fake) -> None:
    fake.keys = [KEY1, KEY2]
    p = inst.InstanceKey(now=lambda: fake.now)
    assert p.key() == KEY1 and p.key() == KEY1
    assert (fake.puts, fake.gets) == (1, 1)
    fake.token = "forgotten"  # hostd forgot it: one new PUT
    p.refused(KEY1)
    assert p.key() == KEY2 and fake.puts == 2
    fake.now += 4 * 60
    assert p.key() == KEY2 and fake.gets == 3  # not refetched 4 minutes into 10
    fake.keys = [KEY1]
    fake.now += 2 * 60  # within 5 minutes of expiry
    assert p.key() == KEY1
    fake.status = 503
    fake.now += 6 * 60  # 12 min in; the key lives until 16
    assert p.key() == KEY1
    fake.now += 5 * 60
    with pytest.raises(inst.InstanceCredentialsError):
        p.key()
    fake.status = 404
    fake.now += 60
    with pytest.raises(NoBotError):
        p.key()


def test_rate_limited_keeps_the_key_and_backs_off(fake: Fake) -> None:
    p = inst.InstanceKey(now=lambda: fake.now)
    assert p.key() == KEY1
    fake.status, fake.retry_after = 429, "120"
    fake.now += 6 * 60  # refresh window
    assert p.key() == KEY1
    n = fake.gets
    fake.now += 60  # inside Retry-After: no new request
    assert p.key() == KEY1 and fake.gets == n


def test_api_401_refetches_once(fake: Fake) -> None:
    fake.keys = [KEY1, KEY2]
    q = QueuesClient(organization_id="o", project_id="p", base_url="http://q.test")
    q.send("que_a", [{"body": "x"}])
    fake.refused.add(f"Bearer {KEY1}")
    q.send("que_a", [{"body": "x"}])
    assert fake.api_auth[-2:] == [f"Bearer {KEY1}", f"Bearer {KEY2}"]
    fake.refused.add(f"Bearer {KEY2}")
    n = len(fake.api_auth)
    with pytest.raises(Exception) as e:
        q.send("que_a", [{"body": "x"}])
    assert getattr(e.value, "status", None) == 401 and len(fake.api_auth) - n == 2


def test_client_uses_the_instance_key(fake: Fake) -> None:
    c = Client(organization_id="o", project_id="p", base_url="http://api.test")
    c.get("run_1")
    assert fake.api_auth == [f"Bearer {KEY1}"]


def test_explicit_key_wins_and_off_instance_is_an_error(fake: Fake, monkeypatch: pytest.MonkeyPatch) -> None:
    Client(api_key="ezgh_explicit", organization_id="o", project_id="p").get("r")
    assert fake.api_auth == ["Bearer ezgh_explicit"] and (fake.puts, fake.gets) == (0, 0)
    monkeypatch.setattr(inst, "PRODUCT_NAME_PATH", "/nonexistent")
    with pytest.raises(ValueError, match="api_key is required"):
        Client(organization_id="o", project_id="p")
    assert (fake.puts, fake.gets) == (0, 0)
