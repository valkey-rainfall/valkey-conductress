"""User and agent identity directory read from a control-plane-local TOML file.

The directory maps a login to a person or agent. A human entry carries the
GitHub account it acts under, its role, its daily runner-minute quota, and the
build sources it may name. Bearer tokens (see :mod:`conductress.control.auth`)
reference a login; this module turns that login into the caller's full identity
so the submit path can apply the provenance gate, quota, and approval rules.

An agent is not a separate account. Its entry names only a login and the human
it acts for (``sponsor``); the GitHub account, role, quota and sources are
inherited from the sponsor at load time and may not be set on the agent entry.
The separate login exists so the agent holds its own bearer token and every
task records which of the two actually submitted it. Quota is therefore keyed
on the GitHub account: an agent and its sponsor share one budget.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

try:  # Python 3.11+
    import tomllib as _toml
except ModuleNotFoundError:  # Python 3.9/3.10
    import tomli as _toml  # type: ignore[no-redef]


VALID_KINDS = {"human", "agent"}
VALID_ROLES = {"owner", "collaborator", "approver"}


@dataclass(frozen=True)
class User:
    """One entry in the user directory."""

    login: str
    github: str
    kind: str
    role: str
    quota_runner_minutes_per_day: int
    sources: tuple[str, ...] = ()
    sponsor: Optional[str] = None

    @property
    def is_owner(self) -> bool:
        return self.role == "owner"

    @property
    def is_approver(self) -> bool:
        # Owners are implicitly approvers; the approval flow treats them alike.
        return self.role in {"approver", "owner"}


@dataclass(frozen=True)
class UserDirectory:
    """All users keyed by login, with quota resolved per GitHub account."""

    users: dict[str, User] = field(default_factory=dict)

    @classmethod
    def from_file(cls, path: Path) -> "UserDirectory":
        with path.open("rb") as stream:
            data = _toml.load(stream)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict) -> "UserDirectory":
        raw_users = data.get("users")
        if not isinstance(raw_users, list):
            raise ValueError("users file must contain a [[users]] array")
        humans: dict[str, User] = {}
        agents: dict[str, str] = {}  # agent login -> sponsor login
        for record in raw_users:
            login, kind = _parse_login_and_kind(record)
            if login in humans or login in agents:
                raise ValueError(f"duplicate user login: {login}")
            if kind == "human":
                humans[login] = _parse_human(record, login)
            else:
                agents[login] = _parse_agent(record, login)
        users: dict[str, User] = dict(humans)
        for login, sponsor_login in agents.items():
            sponsor = humans.get(sponsor_login)
            if sponsor is None:
                if sponsor_login in agents:
                    raise ValueError(f"user {login}: sponsor {sponsor_login} must be a human account")
                raise ValueError(f"user {login}: sponsor {sponsor_login} is not a known user")
            users[login] = User(
                login=login,
                github=sponsor.github,
                kind="agent",
                role=sponsor.role,
                quota_runner_minutes_per_day=sponsor.quota_runner_minutes_per_day,
                sources=sponsor.sources,
                sponsor=sponsor.login,
            )
        return cls(users=users)

    def get(self, login: str) -> Optional[User]:
        return self.users.get(login)

    def require(self, login: str) -> User:
        user = self.users.get(login)
        if user is None:
            raise KeyError(f"unknown user login: {login}")
        return user

    def quota_account(self, login: str) -> str:
        """Return the GitHub account a login's runner-minutes are charged to.

        An agent inherits its sponsor's GitHub account at load time, so this is
        the sponsor's account for an agent and the user's own for a human.
        """
        return self.require(login).github

    def quota_minutes(self, login: str) -> int:
        """Return the daily runner-minute quota for a login's charging account."""
        return self.require(login).quota_runner_minutes_per_day


_COMMON_FIELDS = {"login", "kind"}
_HUMAN_FIELDS = _COMMON_FIELDS | {"github", "role", "quota_runner_minutes_per_day", "sources"}
_AGENT_FIELDS = _COMMON_FIELDS | {"sponsor"}


def _parse_login_and_kind(record: object) -> tuple[str, str]:
    if not isinstance(record, dict):
        raise ValueError("each user entry must be a table")
    login = record.get("login")
    kind = record.get("kind")
    if not isinstance(login, str) or not login:
        raise ValueError("user login must be a non-empty string")
    if kind not in VALID_KINDS:
        raise ValueError(f"user {login}: kind must be one of {sorted(VALID_KINDS)}")
    return login, kind


def _parse_human(record: dict, login: str) -> User:
    unknown = set(record) - _HUMAN_FIELDS
    if unknown:
        if "sponsor" in unknown:
            raise ValueError(f"user {login}: a human must not name a sponsor")
        raise ValueError(f"user {login}: unknown user fields: {', '.join(sorted(unknown))}")
    github = record.get("github")
    role = record.get("role")
    # No quota means no limit (0): a login is held only when an operator gives it a quota.
    quota = record.get("quota_runner_minutes_per_day", 0)
    sources = record.get("sources", [])
    if not isinstance(github, str) or not github:
        raise ValueError(f"user {login}: github must be a non-empty string")
    if role not in VALID_ROLES:
        raise ValueError(f"user {login}: role must be one of {sorted(VALID_ROLES)}")
    if not isinstance(quota, int) or isinstance(quota, bool) or quota < 0:
        raise ValueError(f"user {login}: quota_runner_minutes_per_day must be a non-negative integer")
    if not isinstance(sources, list) or not all(isinstance(item, str) for item in sources):
        raise ValueError(f"user {login}: sources must be a list of strings")
    return User(
        login=login,
        github=github,
        kind="human",
        role=role,
        quota_runner_minutes_per_day=quota,
        sources=tuple(sources),
        sponsor=None,
    )


def _parse_agent(record: dict, login: str) -> str:
    """Validate an agent entry and return its sponsor login.

    An agent acts in its sponsor's name: it may not declare its own GitHub
    account, role, quota or sources, so any of those fields is rejected rather
    than silently ignored.
    """
    unknown = set(record) - _AGENT_FIELDS
    if unknown:
        inherited = sorted(unknown & (_HUMAN_FIELDS - _COMMON_FIELDS))
        if inherited:
            raise ValueError(
                f"user {login}: an agent inherits {', '.join(inherited)} from its sponsor and must not set them"
            )
        raise ValueError(f"user {login}: unknown user fields: {', '.join(sorted(unknown))}")
    sponsor = record.get("sponsor")
    if not sponsor:
        raise ValueError(f"user {login}: an agent must name a sponsor")
    if not isinstance(sponsor, str):
        raise ValueError(f"user {login}: sponsor must be a string")
    return sponsor
