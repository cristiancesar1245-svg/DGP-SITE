from __future__ import annotations

import csv
import json
import os
import re
import secrets
import unicodedata
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation
from io import StringIO
from datetime import timedelta
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request as UrlRequest, urlopen

import mysql.connector
from mysql.connector import IntegrityError
from flask import Flask, Response, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NO_ADMIN_SECTOR = "SEM SETOR ADM"
DISCORD_AUTHORIZE_URL = "https://discord.com/oauth2/authorize"
DISCORD_TOKEN_URL = "https://discord.com/api/v10/oauth2/token"
DISCORD_USER_URL = "https://discord.com/api/v10/users/@me"
DEFAULT_HTTP_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
}


def load_env_file(path: str) -> None:
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


load_env_file(os.path.join(BASE_DIR, ".env"))

app = Flask(__name__)
app.config.update(
    DB_HOST=os.getenv("DGP_DB_HOST", "127.0.0.1"),
    DB_PORT=int(os.getenv("DGP_DB_PORT", "3308")),
    DB_DATABASE=os.getenv("DGP_DB_DATABASE", "dgp"),
    DB_USERNAME=os.getenv("DGP_DB_USERNAME", "root"),
    DB_PASSWORD=os.getenv("DGP_DB_PASSWORD", "12457803"),
    DB_CONNECTION_TIMEOUT=int(os.getenv("DGP_DB_CONNECTION_TIMEOUT", "5")),
    SECRET_KEY=os.getenv("DGP_SECRET_KEY", "dgp-dev-secret-change-me"),
    PERMANENT_SESSION_LIFETIME=timedelta(hours=int(os.getenv("DGP_SESSION_HOURS", "12"))),
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    DISCORD_CLIENT_ID=os.getenv("DGP_DISCORD_CLIENT_ID", "").strip(),
    DISCORD_CLIENT_SECRET=os.getenv("DGP_DISCORD_CLIENT_SECRET", "").strip(),
    DISCORD_REDIRECT_URI=os.getenv("DGP_DISCORD_REDIRECT_URI", "").strip(),
    DISCORD_ALLOWED_IDS=os.getenv("DGP_DISCORD_ALLOWED_IDS", "").strip(),
    DISCORD_ADMIN_IDS=os.getenv("DGP_DISCORD_ADMIN_IDS", "").strip(),
    LOCAL_ADMIN_USER=os.getenv("DGP_LOCAL_ADMIN_USER", "admin").strip() or "admin",
    LOCAL_ADMIN_PASSWORD=os.getenv("DGP_LOCAL_ADMIN_PASSWORD", "admin123").strip() or "admin123",
    DEV_RELOAD=os.getenv("DGP_DEV_RELOAD", "0").strip() == "1",
)
app.config["TEMPLATES_AUTO_RELOAD"] = app.config["DEV_RELOAD"]
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0 if app.config["DEV_RELOAD"] else None
AUTH_SCHEMA_READY = False


@app.after_request
def disable_cache_in_dev(response: Response) -> Response:
    if app.config["DEV_RELOAD"]:
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


def discord_auth_enabled() -> bool:
    return bool(app.config["DISCORD_CLIENT_ID"] and app.config["DISCORD_CLIENT_SECRET"])


def current_auth_user() -> dict:
    return session.get("auth_user") or session.get("discord_user") or {}


def discord_allowed_ids() -> set[str]:
    raw_value = app.config["DISCORD_ALLOWED_IDS"]
    if not raw_value:
        return set()
    return {value.strip() for value in raw_value.split(",") if value.strip()}


def discord_admin_ids() -> set[str]:
    raw_value = app.config["DISCORD_ADMIN_IDS"]
    if not raw_value:
        return set()
    return {value.strip() for value in raw_value.split(",") if value.strip()}


def discord_redirect_uri() -> str:
    return app.config["DISCORD_REDIRECT_URI"] or url_for("discord_callback", _external=True)


def safe_redirect_target(target: str | None) -> str:
    if not target:
        return url_for("dashboard")
    parsed = urlparse(target)
    if parsed.scheme or parsed.netloc:
        return url_for("dashboard")
    if not target.startswith("/"):
        return url_for("dashboard")
    return target


def exchange_discord_code(code: str) -> dict:
    payload = urlencode(
        {
            "client_id": app.config["DISCORD_CLIENT_ID"],
            "client_secret": app.config["DISCORD_CLIENT_SECRET"],
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": discord_redirect_uri(),
        }
    ).encode("utf-8")
    request_headers = {
        **DEFAULT_HTTP_HEADERS,
        "Content-Type": "application/x-www-form-urlencoded",
    }
    request_obj = UrlRequest(DISCORD_TOKEN_URL, data=payload, headers=request_headers, method="POST")
    with urlopen(request_obj, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_discord_user(access_token: str) -> dict:
    request_headers = {
        **DEFAULT_HTTP_HEADERS,
        "Authorization": f"Bearer {access_token}",
    }
    request_obj = UrlRequest(DISCORD_USER_URL, headers=request_headers, method="GET")
    with urlopen(request_obj, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def extract_http_error_payload(error: HTTPError) -> dict:
    try:
        body = error.read().decode("utf-8", errors="replace")
        return json.loads(body)
    except Exception:
        return {}


def discord_avatar_url(user: dict) -> str | None:
    avatar = user.get("avatar")
    if not avatar:
        return None
    return f"https://cdn.discordapp.com/avatars/{user['id']}/{avatar}.png?size=128"


def ensure_auth_schema() -> None:
    global AUTH_SCHEMA_READY
    if AUTH_SCHEMA_READY:
        return

    execute(
        """
        create table if not exists system_users (
            id bigint unsigned not null auto_increment primary key,
            discord_id varchar(32) not null unique,
            discord_username varchar(120) not null,
            discord_global_name varchar(120) null,
            display_name varchar(120) not null,
            login_username varchar(120) null unique,
            password_hash varchar(255) null,
            role varchar(30) not null default 'usuario',
            status varchar(30) not null default 'pendente',
            notes text null,
            last_login_at timestamp null,
            created_at timestamp null,
            updated_at timestamp null
        ) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci
        """
    )
    login_username_column = fetch_one(
        """
        select column_name
        from information_schema.columns
        where table_schema = database()
            and table_name = 'system_users'
            and column_name = 'login_username'
        """
    )
    if not login_username_column:
        try:
            execute("alter table system_users add column login_username varchar(120) null unique after display_name")
        except mysql.connector.Error as error:
            if getattr(error, "errno", None) != 1060:
                raise

    password_hash_column = fetch_one(
        """
        select column_name
        from information_schema.columns
        where table_schema = database()
            and table_name = 'system_users'
            and column_name = 'password_hash'
        """
    )
    if not password_hash_column:
        try:
            execute("alter table system_users add column password_hash varchar(255) null after login_username")
        except mysql.connector.Error as error:
            if getattr(error, "errno", None) != 1060:
                raise

    ensure_default_local_admin()
    AUTH_SCHEMA_READY = True


def ensure_default_local_admin() -> None:
    admin_user = app.config["LOCAL_ADMIN_USER"]
    existing = fetch_one(
        """
        select id
        from system_users
        where role = 'administrador'
            and login_username is not null
        limit 1
        """,
    )
    if existing:
        return

    execute(
        """
        insert into system_users
            (discord_id, discord_username, discord_global_name, display_name, login_username, password_hash, role, status, notes, created_at, updated_at)
        values
            (%s, %s, %s, %s, %s, %s, 'administrador', 'ativo', %s, now(), now())
        """,
        (
            f"local-{admin_user}",
            admin_user,
            "Administrador local",
            "Administrador local",
            admin_user,
            generate_password_hash(app.config["LOCAL_ADMIN_PASSWORD"]),
            "Acesso local inicial do sistema.",
        ),
    )


def access_user_for_discord_id(discord_id: str) -> dict:
    ensure_auth_schema()
    return fetch_one(
        """
        select *
        from system_users
        where discord_id = %s
        """,
        (discord_id,),
    )


def access_user_for_login_username(login_username: str) -> dict:
    ensure_auth_schema()
    return fetch_one(
        """
        select *
        from system_users
        where login_username = %s
        """,
        (login_username,),
    )


def access_user_by_id(user_id: int | None) -> dict:
    if not user_id:
        return {}
    ensure_auth_schema()
    return fetch_one(
        """
        select *
        from system_users
        where id = %s
        """,
        (user_id,),
    )


def bootstrap_admin_access(discord_profile: dict) -> None:
    discord_id = discord_profile.get("id")
    if not discord_id or discord_id not in discord_admin_ids():
        return

    ensure_auth_schema()
    existing = access_user_for_discord_id(discord_id)
    display_name = discord_profile.get("global_name") or discord_profile.get("username") or "Discord"
    if existing:
        execute(
            """
            update system_users
            set discord_username = %s,
                discord_global_name = %s,
                display_name = %s,
                role = 'administrador',
                status = 'ativo',
                updated_at = now()
            where discord_id = %s
            """,
            (
                discord_profile.get("username") or display_name,
                discord_profile.get("global_name"),
                display_name,
                discord_id,
            ),
        )
        return

    execute(
        """
        insert into system_users
            (discord_id, discord_username, discord_global_name, display_name, role, status, created_at, updated_at)
        values
            (%s, %s, %s, %s, 'administrador', 'ativo', now(), now())
        """,
        (
            discord_id,
            discord_profile.get("username") or display_name,
            discord_profile.get("global_name"),
            display_name,
        ),
    )


def sync_system_user(discord_profile: dict) -> dict:
    ensure_auth_schema()
    bootstrap_admin_access(discord_profile)
    discord_id = discord_profile.get("id")
    display_name = discord_profile.get("global_name") or discord_profile.get("username") or "Discord"
    system_user = access_user_for_discord_id(discord_id)
    if not system_user:
        execute(
            """
            insert into system_users
                (discord_id, discord_username, discord_global_name, display_name, role, status, created_at, updated_at)
            values
                (%s, %s, %s, %s, 'usuario', 'pendente', now(), now())
            """,
            (
                discord_id,
                discord_profile.get("username") or display_name,
                discord_profile.get("global_name"),
                display_name,
            ),
        )
        system_user = access_user_for_discord_id(discord_id)
    else:
        execute(
            """
            update system_users
            set discord_username = %s,
                discord_global_name = %s,
                display_name = %s,
                updated_at = now()
            where discord_id = %s
            """,
            (
                discord_profile.get("username") or display_name,
                discord_profile.get("global_name"),
                system_user.get("display_name") or display_name,
                discord_id,
            ),
        )
        system_user = access_user_for_discord_id(discord_id)
    return system_user


def build_session_user(system_user: dict, auth_provider: str, avatar_url: str | None = None) -> dict:
    return {
        "id": system_user.get("discord_id") or system_user.get("id"),
        "username": system_user.get("login_username") or system_user.get("discord_username"),
        "global_name": system_user.get("discord_global_name"),
        "display_name": system_user.get("display_name") or system_user.get("login_username") or system_user.get("discord_username"),
        "avatar_url": avatar_url,
        "role": system_user.get("role"),
        "status": system_user.get("status"),
        "system_user_id": system_user.get("id"),
        "auth_provider": auth_provider,
    }


def current_user_is_admin() -> bool:
    return current_auth_user().get("role") == "administrador"


@app.context_processor
def inject_auth_context() -> dict:
    return {
        "discord_auth_enabled": discord_auth_enabled(),
        "current_user": current_auth_user(),
        "current_user_is_admin": current_user_is_admin(),
    }


@app.before_request
def require_app_login():
    public_endpoints = {
        "login",
        "login_password",
        "discord_login",
        "discord_callback",
        "logout",
        "static",
        "inscricao",
    }
    if request.endpoint in public_endpoints:
        return None

    ensure_auth_schema()

    active_user = current_auth_user()
    if active_user:
        system_user = access_user_by_id(active_user.get("system_user_id"))
        if system_user and system_user.get("status") == "ativo":
            if (
                active_user.get("role") != system_user.get("role")
                or active_user.get("display_name") != system_user.get("display_name")
            ):
                session["auth_user"] = {
                    **active_user,
                    "display_name": system_user.get("display_name") or active_user.get("display_name"),
                    "role": system_user.get("role"),
                    "status": system_user.get("status"),
                    "system_user_id": system_user.get("id"),
                }
            return None

        session.clear()
        return redirect(url_for("login", error="blocked"))

    return redirect(url_for("login", next=request.full_path.rstrip("?")))


def require_admin_access() -> Response | None:
    if not current_user_is_admin():
        return redirect(url_for("dashboard"))
    return None


@contextmanager
def db():
    connection = mysql.connector.connect(
        host=app.config["DB_HOST"],
        port=app.config["DB_PORT"],
        database=app.config["DB_DATABASE"],
        user=app.config["DB_USERNAME"],
        password=app.config["DB_PASSWORD"],
        charset="utf8mb4",
        connection_timeout=app.config["DB_CONNECTION_TIMEOUT"],
    )
    try:
        yield connection
    finally:
        connection.close()


def fetch_all(query: str, params: tuple = ()) -> list[dict]:
    with db() as connection:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, params)
        return cursor.fetchall()


def fetch_one(query: str, params: tuple = ()) -> dict:
    rows = fetch_all(query, params)
    return rows[0] if rows else {}


def execute(query: str, params: tuple = ()) -> None:
    with db() as connection:
        cursor = connection.cursor()
        cursor.execute(query, params)
        connection.commit()


def execute_transaction(statements: list[tuple[str, tuple]]) -> None:
    with db() as connection:
        cursor = connection.cursor()
        for query, params in statements:
            cursor.execute(query, params)
        connection.commit()


def form_text(name: str, required: bool = False) -> str | None:
    value = request.form.get(name, "").strip()
    if required and not value:
        raise ValueError(name)
    return value or None


def form_money(name: str, required: bool = False) -> Decimal | None:
    raw_value = request.form.get(name, "").strip()
    if required and not raw_value:
        raise ValueError(name)
    if not raw_value:
        return None

    normalized = raw_value.replace("R$", "").replace(" ", "")
    if "," in normalized:
        normalized = normalized.replace(".", "").replace(",", ".")
    elif normalized.count(".") == 1 and len(normalized.rsplit(".", 1)[1]) <= 2:
        normalized = normalized
    else:
        normalized = normalized.replace(".", "")

    try:
        return Decimal(normalized)
    except InvalidOperation:
        raise ValueError(name) from None


def form_int(name: str, required: bool = False) -> int | None:
    raw_value = request.form.get(name, "").strip()
    if required and not raw_value:
        raise ValueError(name)
    if not raw_value:
        return None

    try:
        return int(raw_value)
    except ValueError:
        raise ValueError(name) from None


def form_minutes(name: str, required: bool = False) -> int | None:
    raw_value = request.form.get(name, "").strip()
    if required and not raw_value:
        raise ValueError(name)
    if not raw_value:
        return None

    if ":" in raw_value:
        hours, minutes = raw_value.split(":", 1)
        try:
            return (int(hours) * 60) + int(minutes)
        except ValueError:
            raise ValueError(name) from None

    try:
        return int(raw_value)
    except ValueError:
        raise ValueError(name) from None


def normalized_identity(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def ranks_are_compatible(first_rank: str | None, second_rank: str | None) -> bool:
    first = normalized_identity(first_rank)
    second = normalized_identity(second_rank)
    return first == second or first == "outros" or second == "outros"


def ensure_sector_schema() -> None:
    execute_transaction(
        [
            (
                """
                create table if not exists sectors (
                    id bigint unsigned not null auto_increment primary key,
                    name varchar(120) not null unique,
                    description varchar(255) null,
                    status varchar(30) not null default 'ativo',
                    created_at timestamp null,
                    updated_at timestamp null
                ) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci
                """,
                (),
            ),
            (
                """
                insert ignore into sectors (name, status, created_at, updated_at)
                select distinct unit, 'ativo', now(), now()
                from members
                where unit is not null and unit <> ''
                """,
                (),
            ),
            (
                """
                insert ignore into sectors (name, description, status, created_at, updated_at)
                values (%s, 'Policiais sem setor administrativo definido.', 'ativo', now(), now())
                """,
                (NO_ADMIN_SECTOR,),
            ),
            (
                """
                create table if not exists member_sectors (
                    member_id bigint unsigned not null,
                    sector_id bigint unsigned not null,
                    created_at timestamp null,
                    primary key (member_id, sector_id),
                    constraint member_sectors_member_id_foreign
                        foreign key (member_id) references members(id)
                        on delete cascade,
                    constraint member_sectors_sector_id_foreign
                        foreign key (sector_id) references sectors(id)
                        on delete cascade
                ) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci
                """,
                (),
            ),
            (
                """
                insert ignore into member_sectors (member_id, sector_id, created_at)
                select m.id, s.id, now()
                from members m
                inner join sectors s on s.name = m.unit
                where m.unit is not null and m.unit <> ''
                """,
                (),
            ),
        ]
    )


def ensure_financial_schema() -> None:
    column = fetch_one(
        """
        select column_name
        from information_schema.columns
        where table_schema = database()
            and table_name = 'financial_payments'
            and column_name = 'department'
        """
    )
    if not column:
        execute("alter table financial_payments add column department varchar(120) null after source_category")


@app.template_filter("money")
def money(value) -> str:
    if value is None:
        value = 0
    amount = int(Decimal(str(value)))
    return "R$ " + f"{amount:,}".replace(",", ".")


@app.template_filter("duration")
def duration(minutes) -> str:
    minutes = int(minutes or 0)
    return f"{minutes // 60}:{minutes % 60:02d}"


def dashboard_metrics() -> dict:
    return fetch_one(
        """
        select
            (select count(*) from members) as members_count,
            (select count(*) from financial_payments) as payments_count,
            (select count(*) from financial_payments where source_category = 'Horas') as hourly_count,
            (select count(*) from financial_payments where source_category = 'Administrativo') as administrative_count,
            coalesce((select sum(net_amount) from financial_payments), 0) as total_amount,
            coalesce((select sum(net_amount) from financial_payments where source_category = 'Horas'), 0) as hourly_amount,
            coalesce((select sum(net_amount) from financial_payments where source_category = 'Administrativo'), 0) as administrative_amount,
            coalesce((select sum(total_minutes) from financial_payments where source_category = 'Horas'), 0) as total_minutes,
            coalesce((select sum(extra_minutes) from financial_payments where source_category = 'Horas'), 0) as extra_minutes
        """
    )


def sector_options() -> list[dict]:
    ensure_sector_schema()
    return fetch_all(
        """
        select name
        from sectors
        where status = 'ativo'
        order by name asc
        """
    )


def sync_financial_members() -> None:
    execute(
        """
        update financial_payments fp
        inner join members m
            on fp.member_id is null
            and (
                fp.source_name = m.full_name
                or lower(fp.source_name) = lower(m.full_name)
            )
        set fp.member_id = m.id,
            fp.updated_at = now()
        """
    )


def merge_duplicate_members_by_identity() -> int:
    members = fetch_all(
        """
        select id, full_name, `rank`, registration_number, unit, role, status
        from members
        order by id asc
        """
    )
    groups: dict[str, list[dict]] = {}
    for member in members:
        groups.setdefault(normalized_identity(member["full_name"]), []).append(member)

    statements: list[tuple[str, tuple]] = []
    merged_count = 0

    for grouped_members in groups.values():
        if len(grouped_members) < 2:
            continue

        canonical_members: list[dict] = []
        for member in grouped_members:
            canonical = next(
                (
                    existing
                    for existing in canonical_members
                    if ranks_are_compatible(existing["rank"], member["rank"])
                ),
                None,
            )
            if not canonical:
                canonical_members.append(member)
                continue

            canonical_id = canonical["id"]
            duplicate_id = member["id"]
            statements.extend(
                [
                    (
                        """
                        update financial_payments
                        set member_id = %s,
                            updated_at = now()
                        where member_id = %s
                        """,
                        (canonical_id, duplicate_id),
                    ),
                    (
                        """
                        insert ignore into member_sectors (member_id, sector_id, created_at)
                        select %s, sector_id, now()
                        from member_sectors
                        where member_id = %s
                        """,
                        (canonical_id, duplicate_id),
                    ),
                    ("delete from member_sectors where member_id = %s", (duplicate_id,)),
                    (
                        """
                        update department_applications
                        set full_name = %s,
                            registration_number = %s,
                            `rank` = %s,
                            unit = %s,
                            updated_at = now()
                        where lower(full_name) = lower(%s)
                            and registration_number = %s
                        """,
                        (
                            canonical["full_name"],
                            canonical["registration_number"],
                            canonical["rank"],
                            canonical["unit"],
                            member["full_name"],
                            member["registration_number"],
                        ),
                    ),
                    ("delete from members where id = %s", (duplicate_id,)),
                ]
            )
            merged_count += 1

    if statements:
        execute_transaction(statements)

    return merged_count


def merge_member_records(canonical_id: int, duplicate_id: int) -> bool:
    if canonical_id == duplicate_id:
        return False

    canonical = fetch_one(
        """
        select id, full_name, `rank`, registration_number, unit, role
        from members
        where id = %s
        """,
        (canonical_id,),
    )
    duplicate = fetch_one(
        """
        select id, full_name, `rank`, registration_number, unit, role
        from members
        where id = %s
        """,
        (duplicate_id,),
    )
    if not canonical or not duplicate:
        return False

    statements: list[tuple[str, tuple]] = []
    canonical_unit = canonical["unit"]
    canonical_role = canonical["role"]
    if duplicate["unit"] and (not canonical_unit or canonical_unit == NO_ADMIN_SECTOR):
        canonical_unit = duplicate["unit"]
    if duplicate["role"] and (not canonical_role or canonical_role == "Importado do financeiro"):
        canonical_role = duplicate["role"]
    if canonical_unit != canonical["unit"] or canonical_role != canonical["role"]:
        statements.append(
            (
                """
                update members
                set unit = %s,
                    role = %s,
                    updated_at = now()
                where id = %s
                """,
                (canonical_unit, canonical_role, canonical_id),
            )
        )

    statements.extend(
        [
            (
                """
                update financial_payments
                set member_id = %s,
                    source_name = %s,
                    updated_at = now()
                where member_id = %s
                """,
                (canonical_id, canonical["full_name"], duplicate_id),
            ),
            (
                """
                insert ignore into member_sectors (member_id, sector_id, created_at)
                select %s, sector_id, now()
                from member_sectors
                where member_id = %s
                """,
                (canonical_id, duplicate_id),
            ),
            ("delete from member_sectors where member_id = %s", (duplicate_id,)),
            (
                """
                update department_applications
                set full_name = %s,
                    registration_number = %s,
                    `rank` = %s,
                    unit = %s,
                    updated_at = now()
                where lower(full_name) = lower(%s)
                    and registration_number = %s
                """,
                (
                    canonical["full_name"],
                    canonical["registration_number"],
                    canonical["rank"],
                    canonical_unit,
                    duplicate["full_name"],
                    duplicate["registration_number"],
                ),
            ),
            ("delete from members where id = %s", (duplicate_id,)),
        ]
    )
    execute_transaction(statements)
    return True


@app.get("/login")
def login():
    active_user = current_auth_user()
    if active_user:
        system_user = access_user_by_id(active_user.get("system_user_id"))
        if system_user and system_user.get("status") == "ativo":
            return redirect(url_for("dashboard"))
        session.clear()

    return render_template(
        "login.html",
        next_target=safe_redirect_target(request.args.get("next")),
        error=request.args.get("error"),
        error_detail=request.args.get("detail", "").strip(),
        local_admin_user=app.config["LOCAL_ADMIN_USER"],
    )


@app.post("/login")
def login_password():
    ensure_auth_schema()
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")
    next_target = safe_redirect_target(request.form.get("next"))
    if not username or not password:
        return redirect(url_for("login", next=next_target, error="local_missing"))

    system_user = access_user_for_login_username(username)
    if not system_user or not system_user.get("password_hash"):
        return redirect(url_for("login", next=next_target, error="local_invalid"))

    if not check_password_hash(system_user["password_hash"], password):
        return redirect(url_for("login", next=next_target, error="local_invalid"))

    if system_user.get("status") != "ativo":
        error_code = "pending" if system_user.get("status") == "pendente" else "blocked"
        return redirect(url_for("login", next=next_target, error=error_code))

    execute(
        """
        update system_users
        set last_login_at = now(),
            updated_at = now()
        where id = %s
        """,
        (system_user["id"],),
    )
    system_user = access_user_by_id(system_user["id"])
    session.clear()
    session.permanent = True
    session["auth_user"] = build_session_user(system_user, auth_provider="local")
    return redirect(next_target)


@app.get("/login/discord")
def discord_login():
    if not discord_auth_enabled():
        return redirect(url_for("login", error="config"))

    state = secrets.token_urlsafe(24)
    next_target = safe_redirect_target(request.args.get("next"))
    session["discord_oauth_state"] = state
    session["post_login_redirect"] = next_target
    authorize_query = urlencode(
        {
            "client_id": app.config["DISCORD_CLIENT_ID"],
            "redirect_uri": discord_redirect_uri(),
            "response_type": "code",
            "scope": "identify",
            "state": state,
        }
    )
    return redirect(f"{DISCORD_AUTHORIZE_URL}?{authorize_query}")


@app.get("/auth/discord/callback")
def discord_callback():
    if not discord_auth_enabled():
        return redirect(url_for("login", error="config"))

    expected_state = session.get("discord_oauth_state")
    received_state = request.args.get("state", "")
    if not expected_state or received_state != expected_state:
        session.pop("discord_oauth_state", None)
        return redirect(url_for("login", error="state"))

    if request.args.get("error"):
        session.pop("discord_oauth_state", None)
        session.pop("post_login_redirect", None)
        return redirect(url_for("login", error="denied"))

    code = request.args.get("code", "")
    if not code:
        session.pop("discord_oauth_state", None)
        session.pop("post_login_redirect", None)
        return redirect(url_for("login", error="missing_code"))

    try:
        token_payload = exchange_discord_code(code)
        access_token = token_payload["access_token"]
        user = fetch_discord_user(access_token)
    except HTTPError as error:
        session.pop("discord_oauth_state", None)
        session.pop("post_login_redirect", None)
        payload = extract_http_error_payload(error)
        detail = (
            payload.get("error_description")
            or payload.get("error")
            or payload.get("detail")
            or f"HTTP {error.code}"
        )
        return redirect(url_for("login", error="oauth", detail=detail))
    except (KeyError, URLError, TimeoutError, json.JSONDecodeError):
        session.pop("discord_oauth_state", None)
        session.pop("post_login_redirect", None)
        return redirect(url_for("login", error="oauth"))

    discord_id = user.get("id")
    allowed_ids = discord_allowed_ids()
    if allowed_ids and discord_id not in allowed_ids:
        session.clear()
        return redirect(url_for("login", error="not_allowed"))

    system_user = sync_system_user(user)
    if system_user.get("status") != "ativo":
        session.clear()
        error_code = "pending" if system_user.get("status") == "pendente" else "blocked"
        return redirect(url_for("login", error=error_code))

    session.pop("discord_oauth_state", None)
    redirect_target = safe_redirect_target(session.pop("post_login_redirect", None))
    session.permanent = True
    session["auth_user"] = build_session_user(
        system_user,
        auth_provider="discord",
        avatar_url=discord_avatar_url(user),
    )
    execute(
        """
        update system_users
        set last_login_at = now(),
            updated_at = now()
        where id = %s
        """,
        (system_user["id"],),
    )
    return redirect(redirect_target)


@app.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.get("/acessos")
def acessos():
    denied = require_admin_access()
    if denied:
        return denied

    ensure_auth_schema()
    users = fetch_all(
        """
        select *
        from system_users
        order by
            field(status, 'pendente', 'ativo', 'bloqueado'),
            display_name asc,
            id asc
        """
    )
    metrics = fetch_one(
        """
        select
            count(*) as total_users,
            sum(case when status = 'ativo' then 1 else 0 end) as active_users,
            sum(case when status = 'pendente' then 1 else 0 end) as pending_users,
            sum(case when status = 'bloqueado' then 1 else 0 end) as blocked_users
        from system_users
        """
    )
    return render_template(
        "acessos.html",
        users=users,
        metrics=metrics,
        saved=request.args.get("saved"),
        error=request.args.get("error"),
    )


@app.post("/acessos")
def criar_acesso():
    denied = require_admin_access()
    if denied:
        return denied

    ensure_auth_schema()
    try:
        display_name = form_text("display_name", required=True)
        login_username = form_text("login_username", required=True)
        password = request.form.get("password", "").strip()
        role = form_text("role", required=True)
        status = form_text("status", required=True)
        notes = form_text("notes")
    except ValueError:
        return redirect(url_for("acessos", error="missing"))

    if not password:
        return redirect(url_for("acessos", error="missing"))

    try:
        execute(
            """
            insert into system_users
                (discord_id, discord_username, discord_global_name, display_name, login_username, password_hash, role, status, notes, created_at, updated_at)
            values
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
            """,
            (
                f"local-{login_username}",
                login_username,
                None,
                display_name,
                login_username,
                generate_password_hash(password),
                role,
                status,
                notes,
            ),
        )
    except IntegrityError:
        return redirect(url_for("acessos", error="duplicate"))

    return redirect(url_for("acessos", saved="created"))


@app.post("/acessos/<int:user_id>")
def atualizar_acesso(user_id: int):
    denied = require_admin_access()
    if denied:
        return denied

    ensure_auth_schema()
    try:
        display_name = form_text("display_name", required=True)
        login_username = form_text("login_username")
        new_password = request.form.get("new_password", "").strip()
        role = form_text("role", required=True)
        status = form_text("status", required=True)
        notes = form_text("notes")
    except ValueError:
        return redirect(url_for("acessos"))
    statements: list[tuple[str, tuple]] = [
        (
            """
            update system_users
            set display_name = %s,
                login_username = %s,
                role = %s,
                status = %s,
                notes = %s,
                updated_at = now()
            where id = %s
            """,
            (display_name, login_username, role, status, notes, user_id),
        )
    ]
    if new_password:
        statements.append(
            (
                """
                update system_users
                set password_hash = %s,
                    updated_at = now()
                where id = %s
                """,
                (generate_password_hash(new_password), user_id),
            )
        )
    try:
        execute_transaction(statements)
    except IntegrityError:
        return redirect(url_for("acessos", error="duplicate"))
    return redirect(url_for("acessos", saved="1"))


@app.get("/")
def dashboard():
    metrics = dashboard_metrics()
    top_payments = fetch_all(
        """
        select fp.*, m.rank
        from financial_payments fp
        left join members m on m.id = fp.member_id
        order by fp.net_amount desc
        limit 8
        """
    )
    recent_members = fetch_all(
        """
        select *
        from members
        order by created_at desc
        limit 8
        """
    )

    return render_template(
        "dashboard.html",
        metrics=metrics,
        top_payments=top_payments,
        recent_members=recent_members,
    )


@app.get("/financeiro")
def financeiro():
    ensure_sector_schema()
    ensure_financial_schema()
    sync_financial_members()
    sectors = sector_options()
    query = request.args.get("q", "").strip()
    category = request.args.get("category", "").strip()
    selected_sector = request.args.get("sector", "").strip()
    params: list = []
    where = []

    if query:
        where.append(
            """
            (
                fp.source_name like %s
                or m.full_name like %s
                or m.rank like %s
                or fp.functions_label like %s
                or fp.source_category like %s
            )
            """
        )
        like = f"%{query}%"
        params.extend([like, like, like, like, like])

    if category:
        where.append("fp.source_category = %s")
        params.append(category)

    if selected_sector:
        where.append(
            """
            (
                fp.department = %s
                or (
                    fp.department is null
                    and exists (
                        select 1
                        from member_sectors msf
                        inner join sectors sf on sf.id = msf.sector_id
                        where msf.member_id = m.id and sf.name = %s
                    )
                )
            )
            """
        )
        params.extend([selected_sector, selected_sector])

    where_sql = "where " + " and ".join(where) if where else ""
    metrics = dashboard_metrics()
    payments = fetch_all(
        f"""
        select fp.*, m.full_name, m.rank, m.unit, coalesce(fp.department, member_sector_map.sector_names, m.unit) as sector_names_label
        from financial_payments fp
        left join members m on m.id = fp.member_id
        left join (
            select ms.member_id, group_concat(s.name order by s.name separator ' + ') as sector_names
            from member_sectors ms
            inner join sectors s on s.id = ms.sector_id
            group by ms.member_id
        ) member_sector_map on member_sector_map.member_id = m.id
        {where_sql}
        order by fp.net_amount desc, fp.source_name asc
        """,
        tuple(params),
    )
    rank_distribution = fetch_all(
        """
        select coalesce(m.rank, 'Sem posto') as rank_name, count(*) as total, coalesce(sum(fp.net_amount), 0) as amount
        from financial_payments fp
        left join members m on m.id = fp.member_id
        group by coalesce(m.rank, 'Sem posto')
        order by amount desc
        """
    )

    return render_template(
        "financeiro.html",
        metrics=metrics,
        payments=payments,
        rank_distribution=rank_distribution,
        query=query,
        category=category,
        selected_sector=selected_sector,
        sectors=sectors,
        saved=request.args.get("saved"),
        deleted=request.args.get("deleted"),
        error=request.args.get("error"),
    )


@app.get("/relatorio-financeiro")
def relatorio_financeiro():
    sectors = sector_options()
    ensure_financial_schema()
    sync_financial_members()
    selected_sector = request.args.get("sector", "").strip()
    params: list = []
    where = []

    if selected_sector:
        where.append("s.name = %s")
        params.append(selected_sector)

    where.append("(admin_payments.administrative_amount is not null or hour_payments.hourly_amount is not null)")
    where_sql = "where " + " and ".join(where)
    rows = fetch_all(
        f"""
        select
            s.name as sector_name,
            m.id as member_id,
            m.full_name,
            m.`rank`,
            m.registration_number,
            m.role,
            coalesce(admin_payments.administrative_amount, 0) as administrative_amount,
            coalesce(hour_payments.hourly_amount, 0) as hourly_amount,
            coalesce(hour_payments.extra_minutes, 0) as extra_minutes,
            coalesce(admin_payments.administrative_amount, 0) + coalesce(hour_payments.hourly_amount, 0) as total_amount
        from sectors s
        inner join member_sectors ms on ms.sector_id = s.id
        inner join members m on m.id = ms.member_id
        left join (
            select member_id, department, sum(net_amount) as administrative_amount
            from financial_payments
            where source_category = 'Administrativo'
            group by member_id, department
        ) admin_payments on admin_payments.member_id = m.id and admin_payments.department = s.name
        left join (
            select member_id, sum(net_amount) as hourly_amount, sum(extra_minutes) as extra_minutes
            from financial_payments
            where source_category = 'Horas'
            group by member_id
        ) hour_payments on hour_payments.member_id = m.id
        {where_sql}
        order by s.name asc, total_amount desc, m.full_name asc
        """,
        tuple(params),
    )
    if selected_sector:
        report_totals = fetch_one(
            """
            select
                count(distinct fp.member_id) as members_count,
                coalesce(sum(case when fp.source_category = 'Administrativo' then fp.net_amount else 0 end), 0) as administrative_amount,
                coalesce(sum(case when fp.source_category = 'Horas' then fp.net_amount else 0 end), 0) as hourly_amount,
                coalesce(sum(case when fp.source_category = 'Horas' then fp.extra_minutes else 0 end), 0) as extra_minutes
            from financial_payments fp
            left join members m on m.id = fp.member_id
            where (
                fp.department = %s
                or (
                    fp.department is null
                    and exists (
                        select 1
                        from member_sectors ms
                        inner join sectors s on s.id = ms.sector_id
                        where ms.member_id = m.id and s.name = %s
                    )
                )
            )
            """,
            (selected_sector, selected_sector),
        )
    else:
        report_totals = fetch_one(
            """
            select
                count(distinct member_id) as members_count,
                coalesce(sum(case when source_category = 'Administrativo' then net_amount else 0 end), 0) as administrative_amount,
                coalesce(sum(case when source_category = 'Horas' then net_amount else 0 end), 0) as hourly_amount,
                coalesce(sum(case when source_category = 'Horas' then extra_minutes else 0 end), 0) as extra_minutes
            from financial_payments
            """
        )

    report: dict[str, dict] = {}
    totals = {
        "members_count": int(report_totals["members_count"] or 0),
        "administrative_amount": report_totals["administrative_amount"] or Decimal("0"),
        "hourly_amount": report_totals["hourly_amount"] or Decimal("0"),
        "extra_minutes": int(report_totals["extra_minutes"] or 0),
        "total_amount": (report_totals["administrative_amount"] or Decimal("0"))
        + (report_totals["hourly_amount"] or Decimal("0")),
    }
    for row in rows:
        sector = report.setdefault(
            row["sector_name"],
            {
                "name": row["sector_name"],
                "members": [],
                "members_count": 0,
                "administrative_amount": Decimal("0"),
                "hourly_amount": Decimal("0"),
                "extra_minutes": 0,
                "total_amount": Decimal("0"),
            },
        )
        sector["members"].append(row)
        sector["members_count"] += 1
        sector["administrative_amount"] += row["administrative_amount"] or Decimal("0")
        sector["hourly_amount"] += row["hourly_amount"] or Decimal("0")
        sector["extra_minutes"] += int(row["extra_minutes"] or 0)
        sector["total_amount"] += row["total_amount"] or Decimal("0")

    if request.args.get("format") == "csv":
        output = StringIO()
        writer = csv.writer(output, delimiter=";")
        writer.writerow(
            [
                "Setor",
                "Membro",
                "Matricula",
                "Posto",
                "Funcao",
                "Administrativo",
                "Horas extras",
                "Tempo extra",
                "Total",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row["sector_name"],
                    row["full_name"],
                    row["registration_number"],
                    row["rank"],
                    row["role"],
                    row["administrative_amount"],
                    row["hourly_amount"],
                    duration(row["extra_minutes"]),
                    row["total_amount"],
                ]
            )
        response = Response(output.getvalue(), mimetype="text/csv; charset=utf-8")
        response.headers["Content-Disposition"] = "attachment; filename=relatorio-financeiro.csv"
        return response

    return render_template(
        "relatorio_financeiro.html",
        sectors=sectors,
        selected_sector=selected_sector,
        report=list(report.values()),
        totals=totals,
    )


@app.post("/financeiro/<int:payment_id>")
def atualizar_pagamento(payment_id: int):
    ensure_financial_schema()
    query = request.form.get("q", "").strip()
    category_filter = request.form.get("category_filter", "").strip()
    sector_filter = request.form.get("sector_filter", "").strip()
    is_autosave = request.headers.get("X-Requested-With") == "XMLHttpRequest"

    try:
        gross_amount = form_money("gross_amount", required=True)
        deductions = form_money("deductions", required=True)
        status = form_text("status", required=True)
        source_category = form_text("source_category", required=True)
        department = form_text("department")
        total_minutes = form_minutes("total_minutes")
        extra_minutes = form_minutes("extra_minutes")
        function_count = form_int("function_count")
        functions_label = form_text("functions_label")
        notes = form_text("notes")
    except ValueError:
        if is_autosave:
            return {"ok": False, "error": "payment"}, 400
        return redirect(url_for("financeiro", q=query, category=category_filter, sector=sector_filter, error="payment"))

    execute(
        """
        update financial_payments
        set gross_amount = %s,
            deductions = %s,
            net_amount = %s,
            status = %s,
            source_category = %s,
            department = %s,
            total_minutes = %s,
            extra_minutes = %s,
            function_count = %s,
            functions_label = %s,
            notes = %s,
            updated_at = now()
        where id = %s
        """,
        (
            gross_amount,
            deductions,
            gross_amount - deductions,
            status,
            source_category,
            (department or sector_filter) if source_category == "Administrativo" else None,
            total_minutes,
            extra_minutes,
            function_count,
            functions_label,
            notes,
            payment_id,
        ),
    )

    if is_autosave:
        return {"ok": True, "net_amount": money(gross_amount - deductions)}

    return redirect(url_for("financeiro", q=query, category=category_filter, sector=sector_filter, saved="1"))


@app.post("/financeiro/<int:payment_id>/excluir")
def excluir_pagamento(payment_id: int):
    ensure_financial_schema()
    query = request.form.get("q", "").strip()
    category_filter = request.form.get("category_filter", "").strip()
    sector_filter = request.form.get("sector_filter", "").strip()

    execute("delete from financial_payments where id = %s", (payment_id,))
    return redirect(url_for("financeiro", q=query, category=category_filter, sector=sector_filter, deleted="1"))


@app.get("/setores")
def setores():
    ensure_sector_schema()
    sectors = fetch_all(
        """
        select
            s.*,
            count(m.id) as members_count
        from sectors s
        left join members m on m.unit = s.name
        group by s.id, s.name, s.description, s.status, s.created_at, s.updated_at
        order by s.status asc, s.name asc
        """
    )

    return render_template(
        "setores.html",
        sectors=sectors,
        saved=request.args.get("saved"),
        error=request.args.get("error"),
    )


@app.post("/setores")
def criar_setor():
    ensure_sector_schema()

    try:
        name = form_text("name", required=True)
        description = form_text("description")
        status = form_text("status", required=True)
    except ValueError:
        return redirect(url_for("setores", error="missing"))

    try:
        execute(
            """
            insert into sectors (name, description, status, created_at, updated_at)
            values (%s, %s, %s, now(), now())
            """,
            (name, description, status),
        )
    except IntegrityError:
        return redirect(url_for("setores", error="duplicate"))

    return redirect(url_for("setores", saved="created"))


@app.post("/setores/<int:sector_id>")
def atualizar_setor(sector_id: int):
    ensure_sector_schema()

    try:
        name = form_text("name", required=True)
        description = form_text("description")
        status = form_text("status", required=True)
    except ValueError:
        return redirect(url_for("setores", error="missing"))

    current_sector = fetch_one("select name from sectors where id = %s", (sector_id,))
    if not current_sector:
        return redirect(url_for("setores", error="missing"))

    old_name = current_sector["name"]
    try:
        execute_transaction(
            [
                (
                    """
                    update sectors
                    set name = %s,
                        description = %s,
                        status = %s,
                        updated_at = now()
                    where id = %s
                    """,
                    (name, description, status, sector_id),
                ),
                (
                    """
                    update members
                    set unit = %s,
                        updated_at = now()
                    where unit = %s
                    """,
                    (name, old_name),
                ),
                (
                    """
                    update department_applications
                    set unit = %s,
                        updated_at = now()
                    where unit = %s
                    """,
                    (name, old_name),
                ),
                (
                    """
                    update financial_payments
                    set department = %s,
                        updated_at = now()
                    where department = %s
                    """,
                    (name, old_name),
                ),
            ]
        )
    except IntegrityError:
        return redirect(url_for("setores", error="duplicate"))

    return redirect(url_for("setores", saved="updated"))


@app.get("/membros")
def membros():
    ensure_sector_schema()
    sync_financial_members()
    query = request.args.get("q", "").strip()
    selected_sector = request.args.get("sector", "").strip()
    params: list = []
    where = []

    if query:
        where.append(
            """
            (
                full_name like %s
                or registration_number like %s
                or `rank` like %s
                or unit like %s
                or role like %s
                or exists (
                    select 1
                    from member_sectors msq
                    inner join sectors sq on sq.id = msq.sector_id
                    where msq.member_id = members.id and sq.name like %s
                )
            )
            """
        )
        like = f"%{query}%"
        params.extend([like, like, like, like, like, like])

    if selected_sector:
        where.append(
            """
            exists (
                select 1
                from member_sectors mss
                inner join sectors ss on ss.id = mss.sector_id
                where mss.member_id = members.id and ss.name = %s
            )
            """
        )
        params.append(selected_sector)

    where_sql = "where " + " and ".join(where) if where else ""
    sectors = sector_options()
    members = fetch_all(
        f"""
        select
            members.*,
            coalesce(member_sector_map.sector_names, members.unit) as sector_names_label
        from members
        left join (
            select ms.member_id, group_concat(s.name order by s.name separator '||') as sector_names
            from member_sectors ms
            inner join sectors s on s.id = ms.sector_id
            group by ms.member_id
        ) member_sector_map on member_sector_map.member_id = members.id
        {where_sql}
        order by unit asc, full_name asc
        """,
        tuple(params),
    )
    for member in members:
        member["sector_values"] = [
            sector_name
            for sector_name in (member.get("sector_names_label") or "").split("||")
            if sector_name
        ]
    sector_summary = fetch_all(
        """
        select s.name, count(ms.member_id) as total
        from sectors s
        left join member_sectors ms on ms.sector_id = s.id
        group by s.id, s.name
        order by total desc, s.name asc
        """
    )
    member_options = fetch_all(
        """
        select id, full_name, `rank`, registration_number, unit
        from members
        order by full_name asc, `rank` asc, id asc
        """
    )

    return render_template(
        "membros.html",
        members=members,
        member_options=member_options,
        sectors=sectors,
        sector_names=[sector["name"] for sector in sectors],
        sector_summary=sector_summary,
        query=query,
        selected_sector=selected_sector,
        saved=request.args.get("saved"),
        error=request.args.get("error"),
    )


@app.post("/membros/sem-setor-adm")
def marcar_sem_setor_adm():
    ensure_sector_schema()
    query = request.form.get("q", "").strip()
    selected_sector = request.form.get("sector_filter", "").strip()

    execute_transaction(
        [
            (
                """
                update members m
                set m.unit = %s,
                    m.updated_at = now()
                where not exists (
                    select 1
                    from financial_payments fp
                    where fp.member_id = m.id
                        and fp.source_category = 'Administrativo'
                )
                """,
                (NO_ADMIN_SECTOR,),
            ),
            (
                """
                delete ms
                from member_sectors ms
                inner join members m on m.id = ms.member_id
                where m.unit = %s
                """,
                (NO_ADMIN_SECTOR,),
            ),
            (
                """
                insert ignore into member_sectors (member_id, sector_id, created_at)
                select m.id, s.id, now()
                from members m
                inner join sectors s on s.name = %s
                where m.unit = %s
                """,
                (NO_ADMIN_SECTOR, NO_ADMIN_SECTOR),
            ),
        ]
    )

    return redirect(url_for("membros", q=query, sector=selected_sector, saved="no_admin"))


@app.post("/membros/unificar-duplicados")
def unificar_membros_duplicados():
    ensure_sector_schema()
    sync_financial_members()
    query = request.form.get("q", "").strip()
    selected_sector = request.form.get("sector_filter", "").strip()
    merged_count = merge_duplicate_members_by_identity()
    return redirect(
        url_for(
            "membros",
            q=query,
            sector=selected_sector,
            saved="merged",
            merged=merged_count,
        )
    )


@app.post("/membros/acoplar")
def acoplar_membros():
    ensure_sector_schema()
    query = request.form.get("q", "").strip()
    selected_sector = request.form.get("sector_filter", "").strip()

    try:
        canonical_id = int(request.form.get("canonical_member_id", ""))
        duplicate_id = int(request.form.get("duplicate_member_id", ""))
    except ValueError:
        return redirect(url_for("membros", q=query, sector=selected_sector, error="merge_missing"))

    if canonical_id == duplicate_id:
        return redirect(url_for("membros", q=query, sector=selected_sector, error="merge_same"))

    merged = merge_member_records(canonical_id, duplicate_id)
    if not merged:
        return redirect(url_for("membros", q=query, sector=selected_sector, error="merge_missing"))

    return redirect(url_for("membros", q=query, sector=selected_sector, saved="manual_merged"))


@app.post("/membros/<int:member_id>")
def atualizar_membro(member_id: int):
    ensure_sector_schema()
    query = request.form.get("q", "").strip()
    selected_sector = request.form.get("sector_filter", "").strip()
    is_autosave = request.headers.get("X-Requested-With") == "XMLHttpRequest"

    try:
        full_name = form_text("full_name", required=True)
        registration_number = form_text("registration_number", required=True)
        rank = form_text("rank", required=True)
        units = [unit.strip() for unit in request.form.getlist("units") if unit.strip()]
        if not units:
            legacy_unit = form_text("unit")
            units = [legacy_unit] if legacy_unit else []
        if not units:
            raise ValueError("units")
        unit = units[0]
        role = form_text("role", required=True)
        status = form_text("status", required=True)
    except ValueError:
        if is_autosave:
            return {"ok": False, "error": "missing"}, 400
        return redirect(url_for("membros", q=query, sector=selected_sector, error="missing"))

    try:
        statements = [
            (
                """
                update members
                set full_name = %s,
                    registration_number = %s,
                    `rank` = %s,
                    unit = %s,
                    role = %s,
                    status = %s,
                    updated_at = now()
                where id = %s
                """,
                (
                    full_name,
                    registration_number,
                    rank,
                    unit,
                    role,
                    status,
                    member_id,
                ),
            ),
            (
                """
                update financial_payments
                set source_name = %s,
                    updated_at = now()
                where member_id = %s
                """,
                (full_name, member_id),
            ),
            ("delete from member_sectors where member_id = %s", (member_id,)),
        ]
        for sector_name in units:
            statements.append(
                (
                    """
                    insert ignore into sectors (name, status, created_at, updated_at)
                    values (%s, 'ativo', now(), now())
                    """,
                    (sector_name,),
                )
            )
            statements.append(
                (
                    """
                    insert ignore into member_sectors (member_id, sector_id, created_at)
                    select %s, id, now()
                    from sectors
                    where name = %s
                    """,
                    (member_id, sector_name),
                )
            )
        execute_transaction(statements)
    except IntegrityError:
        if is_autosave:
            return {"ok": False, "error": "duplicate"}, 409
        return redirect(url_for("membros", q=query, sector=selected_sector, error="duplicate"))

    if is_autosave:
        return {"ok": True, "primary_sector": unit, "sectors": units}

    return redirect(url_for("membros", q=query, sector=unit, saved="1"))


@app.route("/inscricao", methods=["GET", "POST"])
def inscricao():
    if request.method == "POST":
        form = request.form
        execute(
            """
            insert into department_applications
                (full_name, registration_number, rank, unit, email, phone, motivation, status, submitted_at, created_at, updated_at)
            values
                (%s, %s, %s, %s, %s, %s, %s, 'pendente', now(), now(), now())
            """,
            (
                form.get("full_name"),
                form.get("registration_number"),
                form.get("rank"),
                form.get("unit"),
                form.get("email"),
                form.get("phone"),
                form.get("motivation"),
            ),
        )
        return redirect(url_for("inscricoes", saved="1"))

    return render_template("inscricao.html")


@app.get("/inscricoes")
def inscricoes():
    applications = fetch_all(
        """
        select *
        from department_applications
        order by created_at desc
        """
    )
    return render_template("inscricoes.html", applications=applications, saved=request.args.get("saved"))


if __name__ == "__main__":
    port = int(os.getenv("DGP_PORT", "8080"))
    app.run(
        host="127.0.0.1",
        port=port,
        debug=app.config["DEV_RELOAD"],
        use_reloader=app.config["DEV_RELOAD"],
    )
