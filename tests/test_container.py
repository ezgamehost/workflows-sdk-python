import json
import threading
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

import ezgh_workflows._container as ctr
import ezgh_workflows._instance as inst
from ezgh_workflows import Client, ContainerCredentialsError, WorkflowsApiError

POD_KEY = "ezgh_" + "p" * 40
KEY1 = "ezgh_" + "a" * 40
KEY2 = "ezgh_" + "b" * 40


class Agent:
    """A fake pod identity agent (/v1/credentials) and API on one local server."""

    def __init__(self) -> None:
        self.now = datetime.now(UTC).timestamp()
        self.gets = 0
        self.tokens: list[str] = []  # the bearer tokens the agent saw
        self.keys = [POD_KEY]  # handed out in order, the last repeated
        self.status = 0
        self.retry_after = ""
        self.during: Callable[[], None] = lambda: None  # runs while the agent answers: a slow fetch
        self.refused: set[str] = set()
        self.api_auth: list[str] = []
        agent = self

        class H(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                auth = self.headers.get("Authorization", "")
                if self.path != "/v1/credentials":
                    agent.api_auth.append(auth)
                    if auth in agent.refused:
                        return self._send(401, {}, b'{"error":{"code":"unauthorized","message":"no"}}')
                    return self._send(200, {}, b'{"id":"run_1"}')
                agent.gets += 1
                agent.during()
                agent.tokens.append(auth.removeprefix("Bearer "))
                if agent.status:
                    h = {"Retry-After": agent.retry_after} if agent.retry_after else {}
                    return self._send(agent.status, h, b"")
                k = agent.keys.pop(0) if len(agent.keys) > 1 else agent.keys[0]
                exp = datetime.fromtimestamp(agent.now + 600, UTC).isoformat().replace("+00:00", "Z")
                self._send(200, {}, json.dumps({"apiKey": k, "expiresAt": exp, "bot": "bot_pod"}).encode())

            def _send(self, status: int, headers: dict[str, str], body: bytes) -> None:
                self.send_response(status)
                for k, v in {**headers, "Content-Length": str(len(body))}.items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args: object) -> None:
                pass

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}"
        self.uri = self.url + "/v1/credentials"
        threading.Thread(target=self.srv.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True).start()


@pytest.fixture
def agent() -> Iterator[Agent]:
    a = Agent()
    yield a
    a.srv.shutdown()
    a.srv.server_close()


def token_file(tmp_path: Path, token: str) -> str:
    p = tmp_path / "token"
    p.write_text(token + "\n")
    return str(p)


def in_pod(monkeypatch: pytest.MonkeyPatch, uri: str, file: str) -> None:
    """Set the variables EZGH Kubernetes injects."""
    monkeypatch.setenv(ctr.URI_ENV, uri)
    monkeypatch.setenv(ctr.TOKEN_FILE_ENV, file)


def on_instance(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> list[str]:
    """Make this an EZGH instance whose metadata service records the requests it gets."""
    p = tmp_path / "product_name"
    p.write_text("EZGH Compute\n")
    monkeypatch.setattr(inst, "PRODUCT_NAME_PATH", str(p))
    hits: list[str] = []
    monkeypatch.setattr(inst, "request", lambda m, url, *a: hits.append(url))
    return hits


def test_container_credentials_win_over_the_instance(
    agent: Agent, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    meta = on_instance(monkeypatch, tmp_path)
    in_pod(monkeypatch, agent.uri, token_file(tmp_path, "sa"))
    Client(organization_id="o", project_id="p", base_url=agent.url).get("run_1")
    assert (agent.api_auth, agent.tokens, meta) == ([f"Bearer {POD_KEY}"], ["sa"], [])


def test_an_explicit_key_wins_over_the_container(agent: Agent, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    in_pod(monkeypatch, agent.uri, token_file(tmp_path, "sa"))
    Client(api_key="ezgh_explicit", organization_id="o", project_id="p", base_url=agent.url).get("run_1")
    assert (agent.api_auth, agent.gets) == (["Bearer ezgh_explicit"], 0)


def test_refreshes_5_minutes_before_expiry(agent: Agent, tmp_path: Path) -> None:
    agent.keys = [KEY1, KEY2]
    p = ctr.ContainerKey(agent.uri, token_file(tmp_path, "sa"), now=lambda: agent.now)
    assert p.key() == KEY1
    agent.now += 4 * 60
    assert p.key() == KEY1 and agent.gets == 1  # not refetched 4 minutes into 10
    agent.now += 2 * 60  # within 5 minutes of expiry
    assert p.key() == KEY2 and agent.gets == 2


def test_a_failed_fetch_keeps_the_key_until_expiry(agent: Agent, tmp_path: Path) -> None:
    p = ctr.ContainerKey(agent.uri, token_file(tmp_path, "sa"), now=lambda: agent.now)
    assert p.key() == POD_KEY
    agent.status, agent.retry_after = 503, "120"
    agent.now += 6 * 60
    assert p.key() == POD_KEY
    n = agent.gets
    agent.now += 60  # inside Retry-After: nothing is asked
    assert p.key() == POD_KEY and agent.gets == n
    agent.now += 4 * 60  # expired
    with pytest.raises(ContainerCredentialsError, match="answered 503"):
        p.key()


def test_a_slow_failed_fetch_reads_the_clock_again(agent: Agent, tmp_path: Path) -> None:
    p = ctr.ContainerKey(agent.uri, token_file(tmp_path, "sa"), now=lambda: agent.now)
    assert p.key() == POD_KEY
    agent.now += 6 * 60  # refresh due; the key lives 4 more minutes
    agent.status = 503

    def slow() -> None:
        agent.now += 5 * 60  # the fetch outlives the key

    agent.during = slow
    with pytest.raises(ContainerCredentialsError, match="answered 503"):
        p.key()
    agent.during = lambda: None
    n = agent.gets
    agent.now += 4  # within 5 s of the fetch's end: nothing is asked
    with pytest.raises(ContainerCredentialsError, match="answered 503"):
        p.key()
    assert agent.gets == n


@pytest.mark.parametrize("retry_after", ["100000", "inf"])
def test_retry_after_is_capped_and_lasts_only_for_its_answer(agent: Agent, tmp_path: Path, retry_after: str) -> None:
    file = token_file(tmp_path, "sa")
    p = ctr.ContainerKey(agent.uri, file, now=lambda: agent.now)
    agent.status, agent.retry_after = 503, retry_after
    with pytest.raises(ContainerCredentialsError, match="answered 503"):
        p.key()
    agent.now += 301
    Path(file).unlink()  # fails with no answer from the agent: the default 5 s backoff
    with pytest.raises(ContainerCredentialsError, match=ctr.TOKEN_FILE_ENV):
        p.key()
    Path(file).write_text("sa\n")
    agent.status = 0
    agent.now += 6
    assert p.key() == POD_KEY


@pytest.mark.parametrize("token", ["sa\nX-Evil: 1", "s\u00e4\u20ac"])
def test_a_token_that_cant_be_a_header_is_a_credentials_error(agent: Agent, tmp_path: Path, token: str) -> None:
    with pytest.raises(ContainerCredentialsError, match="isn't a valid header"):
        ctr.ContainerKey(agent.uri, token_file(tmp_path, token)).key()
    assert agent.gets == 0


def test_a_401_refetches_once(agent: Agent, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    agent.keys = [KEY1, KEY2]
    in_pod(monkeypatch, agent.uri, token_file(tmp_path, "sa"))
    c = Client(organization_id="o", project_id="p", base_url=agent.url)
    c.get("run_1")
    agent.refused.add(f"Bearer {KEY1}")
    c.get("run_1")
    assert agent.api_auth[-2:] == [f"Bearer {KEY1}", f"Bearer {KEY2}"]
    agent.refused.add(f"Bearer {KEY2}")
    n = len(agent.api_auth)
    with pytest.raises(WorkflowsApiError) as e:
        c.get("run_1")
    assert e.value.status == 401 and len(agent.api_auth) - n == 2


def test_a_rotated_token_file_is_read_on_the_next_fetch(agent: Agent, tmp_path: Path) -> None:
    file = token_file(tmp_path, "sa-1")
    p = ctr.ContainerKey(agent.uri, file)
    p.key()
    Path(file).write_text("sa-2\n")
    p.refused(POD_KEY)
    p.key()
    assert agent.tokens == ["sa-1", "sa-2"]


def test_a_non_link_local_uri_is_refused_without_a_request(
    agent: Agent, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    port = agent.srv.server_address[1]
    for bad in (
        f"http://localhost:{port}/v1/credentials",
        f"https://127.0.0.1:{port}/",
        "http://169.254.169.254/",
        "http://u:p@127.0.0.1/",
        "http://10.0.0.1/",
        "nonsense",
    ):
        with pytest.raises(ContainerCredentialsError, match=r"must be http://169\.254\.170\.23"):
            ctr.check_container_uri(bad)
    for ok in (
        "http://169.254.170.23/v1/credentials",
        "http://[fd65:7a67:6800::23]/v1/credentials",
        "http://127.0.0.1:8080/",
        "http://[::1]/",
    ):
        ctr.check_container_uri(ok)
    in_pod(monkeypatch, f"http://localhost:{port}/v1/credentials", token_file(tmp_path, "sa"))
    with pytest.raises(ContainerCredentialsError, match=r"must be http://169\.254\.170\.23"):
        Client(organization_id="o", project_id="p", base_url=agent.url).get("run_1")
    assert (agent.gets, agent.api_auth) == (0, [])


def test_a_missing_token_file_is_an_error_not_the_instance(
    agent: Agent, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    meta = on_instance(monkeypatch, tmp_path)
    in_pod(monkeypatch, agent.uri, str(tmp_path / "no-such-token"))
    with pytest.raises(ContainerCredentialsError, match=ctr.TOKEN_FILE_ENV):
        Client(organization_id="o", project_id="p", base_url=agent.url).get("run_1")
    assert (agent.gets, agent.api_auth, meta) == (0, [], [])
