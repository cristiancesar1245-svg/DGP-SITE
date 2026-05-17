from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import secrets
import unicodedata
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from io import StringIO
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request as UrlRequest, urlopen

import mysql.connector
from mysql.connector import IntegrityError
from flask import Flask, Response, g, redirect, render_template, request, session, url_for
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash, generate_password_hash


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NO_ADMIN_SECTOR = "SEM SETOR ADM"
FINANCIAL_REFERENCE_MIN = date(2026, 3, 1)
FINANCIAL_REFERENCE_MAX = date(2026, 12, 1)
DISCORD_AUTHORIZE_URL = "https://discord.com/oauth2/authorize"
DISCORD_TOKEN_URL = "https://discord.com/api/v10/oauth2/token"
DISCORD_USER_URL = "https://discord.com/api/v10/users/@me"
DEFAULT_HTTP_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
}
HOURLY_IMPORT_PATTERN = re.compile(
    r"^@(.+?)\s+-\s+(\d+):(\d+)(?:\s+\(extra\s+(\d+):(\d+)\))?\s+-\s*R\$\s*([\d.,]+)(?:\s+.*)?$"
)
HOURLY_IMPORT_WITH_ID_PATTERN = re.compile(
    r"^@(.+?)\s+-\s+(\d+)\s+-\s+(\d+):(\d+)(?:\s+\(extra\s+(\d+):(\d+)\))?\s+-\s*R\$\s*([\d.,]+)(?:\s+.*)?$"
)
ADMIN_IMPORT_PATTERN = re.compile(
    r"^@(.+?)\s+-\s+(\d+)\s+fun\S*\s+\((.*?)\)\s+-\s*R\$\s*([\d.,]+)$"
)
ADMIN_BULLET_PATTERN = re.compile(r"^\*?\s*@(.+?)(?:\s+-\s+(\d+))?\s+\(\*\*(\d+)\*\*\)\s*$")
ADMIN_VALUE_PATTERN = re.compile(r"^valor\s*:\s*([\d.,]+)$", re.IGNORECASE)
ADMIN_PENDING_MEMBER_PATTERN = re.compile(r"^\*?\s*@?(.+?)(?:\s*-\s*(\d+))?(?:\s+\(\*\*(\d+)\*\*\))?\s*$")
IMPORT_MEMBER_NAME_ALIASES = {
    "Cabo A Ramirez": "Cabo Adriel",
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

def normalized_session_cookie_samesite() -> str:
    raw_value = os.getenv("DGP_SESSION_COOKIE_SAMESITE", "Lax").strip().lower()
    if raw_value == "none":
        return "None"
    if raw_value == "strict":
        return "Strict"
    return "Lax"


def inferred_cookie_domain() -> str | None:
    explicit_domain = os.getenv("DGP_SESSION_COOKIE_DOMAIN", "").strip()
    return explicit_domain or None


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
    SESSION_COOKIE_SAMESITE=normalized_session_cookie_samesite(),
    SESSION_COOKIE_DOMAIN=inferred_cookie_domain(),
    DISCORD_CLIENT_ID=os.getenv("DGP_DISCORD_CLIENT_ID", "").strip(),
    DISCORD_CLIENT_SECRET=os.getenv("DGP_DISCORD_CLIENT_SECRET", "").strip(),
    DISCORD_REDIRECT_URI=os.getenv("DGP_DISCORD_REDIRECT_URI", "").strip(),
    PUBLIC_BASE_URL=os.getenv("DGP_PUBLIC_BASE_URL", "").strip().rstrip("/"),
    DISCORD_ALLOWED_IDS=os.getenv("DGP_DISCORD_ALLOWED_IDS", "").strip(),
    DISCORD_ADMIN_IDS=os.getenv("DGP_DISCORD_ADMIN_IDS", "").strip(),
    LOCAL_ADMIN_USER=os.getenv("DGP_LOCAL_ADMIN_USER", "admin").strip() or "admin",
    LOCAL_ADMIN_PASSWORD=os.getenv("DGP_LOCAL_ADMIN_PASSWORD", "admin123").strip() or "admin123",
    DEV_RELOAD=os.getenv("DGP_DEV_RELOAD", "0").strip() == "1",
    TRUST_PROXY=os.getenv("DGP_TRUST_PROXY", "1").strip() != "0",
    ENFORCE_PUBLIC_BASE_URL=os.getenv("DGP_ENFORCE_PUBLIC_BASE_URL", "0").strip() == "1",
    SESSION_COOKIE_SECURE=os.getenv("DGP_SESSION_COOKIE_SECURE", "0").strip() == "1",
    PREFERRED_URL_SCHEME=os.getenv("DGP_PREFERRED_URL_SCHEME", "https"),
)
app.config["TEMPLATES_AUTO_RELOAD"] = app.config["DEV_RELOAD"]
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0 if app.config["DEV_RELOAD"] else None
if app.config["TRUST_PROXY"]:
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1)

static_version = os.getenv("DGP_STATIC_VERSION", "").strip()
if not static_version:
    css_path = os.path.join(BASE_DIR, "python_app", "static", "app.css")
    try:
        static_version = str(int(os.path.getmtime(css_path)))
    except OSError:
        static_version = "1"
app.config["STATIC_ASSET_VERSION"] = static_version

AUTH_SCHEMA_READY = False
AUDIT_SCHEMA_READY = False
SENSITIVE_AUDIT_FIELDS = {
    "password",
    "new_password",
    "password_hash",
    "client_secret",
    "code",
    "state",
}
AUDIT_FIELD_LABELS = {
    "display_name": "Nome",
    "login_username": "Login",
    "role": "Perfil",
    "role_label": "Cargo exibido",
    "status": "Status",
    "notes": "Observacoes",
    "full_name": "Nome",
    "registration_number": "Matricula",
    "rank": "Posto",
    "unit": "Unidade",
    "department": "Departamento",
    "description": "Descricao",
    "name": "Nome",
    "email": "Email",
    "phone": "Telefone",
    "motivation": "Motivo",
    "source_name": "Origem",
    "source_category": "Categoria",
    "gross_amount": "Valor bruto",
    "deductions": "Descontos",
    "net_amount": "Valor liquido",
    "total_minutes": "Minutos totais",
    "extra_minutes": "Minutos extras",
    "function_count": "Qtd. funcoes",
    "functions_label": "Funcoes",
    "old_name": "Nome anterior",
    "sectors": "Setores",
    "affected_count": "Registros afetados",
    "affected_members": "Membros afetados",
    "target_sector": "Setor de destino",
    "merged_count": "Unificacoes",
    "canonical_member_name": "Cadastro principal",
    "duplicate_member_name": "Cadastro secundario",
}
AUDIT_ENDPOINT_LABELS = {
    "dashboard": "painel principal",
    "financeiro": "painel financeiro",
    "relatorio_financeiro": "relatorio financeiro",
    "demonstrativo_financeiro": "demonstrativo financeiro",
    "setores": "gestao de setores",
    "membros": "cadastro de membros",
    "acessos": "gestao de acessos",
    "auditoria": "auditoria do sistema",
    "inscricao": "formulario de inscricao",
    "inscricoes": "painel de inscricoes",
    "login": "tela de login",
    "discord_login": "autenticacao via Discord",
    "discord_callback": "retorno da autenticacao via Discord",
}
AUDIT_POST_OPERATION_LABELS = {
    "login_password": "tentativa de login local",
    "criar_acesso": "cadastro de usuario",
    "atualizar_acesso": "edicao de usuario",
    "atualizar_pagamento": "atualizacao de pagamento",
    "excluir_pagamento": "exclusao de pagamento",
    "processar_financeiro_mes": "processamento mensal financeiro",
    "excluir_processamento_mes": "exclusao de processamento mensal financeiro",
    "criar_setor": "cadastro de setor",
    "atualizar_setor": "edicao de setor",
    "marcar_sem_setor_adm": "marcacao de membros sem setor ADM",
    "unificar_membros_duplicados": "unificacao de membros duplicados",
    "acoplar_membros": "acoplamento de membros",
    "atualizar_membro": "edicao de membro",
    "inscricao": "envio de inscricao",
}
AUDIT_REASON_LABELS = {
    "credenciais_ausentes": "credenciais ausentes",
    "usuario_invalido": "usuario invalido",
    "senha_invalida": "senha invalida",
    "pending": "usuario pendente",
    "blocked": "usuario bloqueado",
    "discord_nao_autorizado": "Discord nao autorizado",
    "configuracao_ausente": "configuracao ausente",
    "state_invalido": "state invalido",
    "acesso_negado": "acesso negado",
    "codigo_ausente": "codigo ausente",
    "http_error": "erro HTTP",
    "resposta_invalida": "resposta invalida",
}


@app.after_request
def disable_cache_in_dev(response: Response) -> Response:
    no_cache_endpoints = {
        "login",
        "login_password",
        "discord_login",
        "discord_callback",
        "logout",
    }

    if app.config["DEV_RELOAD"] or request.endpoint in no_cache_endpoints:
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


def discord_auth_enabled() -> bool:
    return bool(app.config["DISCORD_CLIENT_ID"] and app.config["DISCORD_CLIENT_SECRET"])


def display_role_label(role: str | None, auth_provider: str | None = None, role_label: str | None = None) -> str:
    custom_label = (role_label or "").strip()
    if custom_label:
        return custom_label

    normalized_role = (role or "").strip().lower()
    if normalized_role == "administrador":
        return "Diretor DGP"
    if normalized_role == "desenvolvedor":
        return "Desenvolvedor DGP"

    if auth_provider:
        return f"{role or 'usuario'} | {auth_provider}"
    return role or "usuario"


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
    session_redirect_uri = session.get("discord_oauth_redirect_uri")
    if session_redirect_uri:
        return session_redirect_uri

    configured_redirect_uri = app.config["DISCORD_REDIRECT_URI"]
    if configured_redirect_uri:
        return configured_redirect_uri

    return url_for("discord_callback", _external=True)


def safe_redirect_target(target: str | None) -> str:
    if not target:
        return url_for("dashboard")
    parsed = urlparse(target)
    if parsed.scheme or parsed.netloc:
        return url_for("dashboard")
    if not target.startswith("/"):
        return url_for("dashboard")
    normalized_path = parsed.path.rstrip("/") or "/"
    if normalized_path in {"/", "/login", "/login/discord", "/auth/discord/callback", "/logout"}:
        return url_for("dashboard")
    return target


def render_login_page(error: str | None = None, next_target: str | None = None, error_detail: str = "") -> str:
    return render_template(
        "login.html",
        next_target=safe_redirect_target(next_target or request.args.get("next")),
        error=error if error is not None else request.args.get("error"),
        error_detail=error_detail or request.args.get("detail", "").strip(),
        local_admin_user=app.config["LOCAL_ADMIN_USER"],
    )


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


def ensure_audit_schema() -> None:
    global AUDIT_SCHEMA_READY
    if AUDIT_SCHEMA_READY:
        return

    execute(
        """
        create table if not exists audit_logs (
            id bigint unsigned not null auto_increment primary key,
            event_type varchar(40) not null,
            action varchar(120) not null,
            endpoint varchar(120) null,
            request_path varchar(255) null,
            request_method varchar(10) null,
            response_status smallint unsigned null,
            actor_system_user_id bigint unsigned null,
            actor_username varchar(120) null,
            actor_display_name varchar(120) null,
            actor_role varchar(30) null,
            actor_auth_provider varchar(30) null,
            target_type varchar(60) null,
            target_id varchar(120) null,
            target_label varchar(255) null,
            ip_address varchar(64) null,
            user_agent varchar(255) null,
            details_json longtext null,
            created_at timestamp not null default current_timestamp,
            index audit_logs_created_at_idx (created_at desc),
            index audit_logs_event_type_idx (event_type),
            index audit_logs_actor_idx (actor_system_user_id),
            index audit_logs_action_idx (action)
        ) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci
        """
    )
    AUDIT_SCHEMA_READY = True


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
            role_label varchar(120) null,
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

    role_label_column = fetch_one(
        """
        select column_name
        from information_schema.columns
        where table_schema = database()
            and table_name = 'system_users'
            and column_name = 'role_label'
        """
    )
    if not role_label_column:
        try:
            execute("alter table system_users add column role_label varchar(120) null after role")
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
            (discord_id, discord_username, discord_global_name, display_name, login_username, password_hash, role, role_label, status, notes, created_at, updated_at)
        values
            (%s, %s, %s, %s, %s, %s, 'administrador', %s, 'ativo', %s, now(), now())
        """,
        (
            f"local-{admin_user}",
            admin_user,
            "Administrador local",
            "Administrador local",
            admin_user,
            generate_password_hash(app.config["LOCAL_ADMIN_PASSWORD"]),
            "Diretor DGP",
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
        existing_role = (existing.get("role") or "").strip().lower()
        existing_status = (existing.get("status") or "").strip().lower()
        next_role = existing.get("role") or "administrador"
        next_status = existing.get("status") or "ativo"
        next_role_label = existing.get("role_label")

        # Bootstrap automatico deve liberar apenas o primeiro acesso.
        # Depois que o perfil for ajustado manualmente em Acessos, o login
        # via Discord nao pode sobrescrever papel, cargo exibido ou status.
        if existing_role in {"", "usuario"} and existing_status in {"", "pendente"}:
            next_role = "administrador"
            next_status = "ativo"
            next_role_label = next_role_label or "Diretor DGP"
        elif not next_role_label:
            if next_role == "administrador":
                next_role_label = "Diretor DGP"
            elif next_role == "desenvolvedor":
                next_role_label = "Desenvolvedor DGP"

        execute(
            """
            update system_users
            set discord_username = %s,
                discord_global_name = %s,
                display_name = %s,
                role = %s,
                role_label = %s,
                status = %s,
                updated_at = now()
            where discord_id = %s
            """,
            (
                discord_profile.get("username") or display_name,
                discord_profile.get("global_name"),
                existing.get("display_name") or display_name,
                next_role,
                next_role_label,
                next_status,
                discord_id,
            ),
        )
        return

    execute(
        """
        insert into system_users
            (discord_id, discord_username, discord_global_name, display_name, role, role_label, status, created_at, updated_at)
        values
            (%s, %s, %s, %s, 'administrador', %s, 'ativo', now(), now())
        """,
        (
            discord_id,
            discord_profile.get("username") or display_name,
            discord_profile.get("global_name"),
            display_name,
            "Diretor DGP",
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
        "role_label": system_user.get("role_label"),
        "status": system_user.get("status"),
        "system_user_id": system_user.get("id"),
        "auth_provider": auth_provider,
    }


def current_user_role() -> str:
    return (current_auth_user().get("role") or "").strip().lower()


def current_user_is_developer() -> bool:
    return current_user_role() == "desenvolvedor"


def current_user_is_admin() -> bool:
    return current_user_role() in {"administrador", "desenvolvedor"}


def current_user_can_manage_access() -> bool:
    return current_user_is_developer()


def current_user_can_create_access() -> bool:
    return current_user_is_developer()


def current_user_can_view_audit() -> bool:
    return current_user_is_developer()


def current_user_can_edit() -> bool:
    return current_user_is_admin()


def audit_actor_snapshot(user: dict | None = None) -> dict:
    actor = user or current_auth_user() or {}
    return {
        "system_user_id": actor.get("system_user_id"),
        "username": actor.get("username"),
        "display_name": actor.get("display_name"),
        "role": actor.get("role"),
        "auth_provider": actor.get("auth_provider"),
    }


def client_ip_address() -> str:
    forwarded_for = request.headers.get("X-Forwarded-For", "").strip()
    if forwarded_for:
        return forwarded_for.split(",", 1)[0].strip()
    real_ip = request.headers.get("X-Real-IP", "").strip()
    if real_ip:
        return real_ip
    return request.remote_addr or ""


def audit_json_value(value):
    if isinstance(value, dict):
        return {str(key): audit_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [audit_json_value(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def sanitized_payload(source: dict | None) -> dict:
    sanitized: dict[str, str] = {}
    for key, value in (source or {}).items():
        if key in SENSITIVE_AUDIT_FIELDS:
            sanitized[key] = "[hidden]"
            continue
        text = str(value).strip()
        if len(text) > 240:
            text = text[:237] + "..."
        sanitized[key] = text
    return sanitized


def request_payload_snapshot() -> dict:
    payload: dict[str, dict] = {}
    if request.args:
        payload["query"] = sanitized_payload(request.args.to_dict(flat=True))
    if request.form:
        payload["form"] = sanitized_payload(request.form.to_dict(flat=True))
    return payload


def changed_fields(before: dict | None, after: dict) -> dict[str, dict]:
    changes: dict[str, dict] = {}
    original = before or {}
    for key, new_value in after.items():
        old_value = original.get(key)
        if old_value != new_value:
            changes[key] = {"antes": old_value, "depois": new_value}
    return changes


def audit_field_label(field_name: str) -> str:
    if field_name in AUDIT_FIELD_LABELS:
        return AUDIT_FIELD_LABELS[field_name]
    text = field_name.replace("_", " ").strip()
    return text[:1].upper() + text[1:] if text else "Campo"


def audit_detail_value(value) -> str:
    if isinstance(value, bool):
        return "Sim" if value else "Nao"
    if value is None:
        return "-"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return ", ".join(audit_detail_value(item) for item in value) or "-"
    if isinstance(value, str):
        text = value.strip()
        return text or "-"
    return json.dumps(value, ensure_ascii=False)


def audit_action_label(action: str | None) -> str:
    labels = {
        "usuario_criado": "Criou usuario",
        "usuario_atualizado": "Atualizou usuario",
        "pagamento_atualizado": "Atualizou pagamento",
        "pagamento_excluido": "Excluiu pagamento",
        "setor_criado": "Criou setor",
        "setor_atualizado": "Atualizou setor",
        "membro_atualizado": "Atualizou membro",
        "membros_marcados_sem_setor_adm": "Marcou membros sem setor ADM",
        "duplicados_unificados": "Unificou membros duplicados",
        "membros_acoplados": "Acoplou cadastros de membro",
        "inscricao_criada": "Criou inscricao",
        "login_local_sucesso": "Realizou login local",
        "login_local_falhou": "Falha no login local",
        "login_local_negado": "Login local negado",
        "oauth_discord_iniciado": "Iniciou login via Discord",
        "logout": "Realizou logout",
        "oauth_discord_indisponivel": "Falha no login Discord",
        "oauth_discord_falhou": "Falha no login Discord",
        "login_discord_negado": "Login via Discord negado",
        "login_discord_sucesso": "Entrou via Discord",
        "acessou_página": "Acessou pagina",
        "enviou_formulário": "Enviou formulario",
        "requisição": "Executou requisicao",
    }
    labels.update(
        {
            "acessou_pagina": "Acessou pagina",
            "enviou_formulario": "Enviou formulario",
            "requisicao": "Executou requisicao",
            "vinculos_financeiros_sincronizados": "Sincronizou vinculos financeiros",
        }
    )
    if action in labels:
        return labels[action]
    text = (action or "movimentou").replace("_", " ").strip()
    return text[:1].upper() + text[1:] if text else "Movimentou"


def audit_change_items(details: dict | None) -> list[dict]:
    items: list[dict] = []
    raw_changes = details.get("changes") if isinstance(details, dict) else None
    if isinstance(raw_changes, dict):
        for field_name, change in raw_changes.items():
            if not isinstance(change, dict):
                continue
            items.append(
                {
                    "label": audit_field_label(field_name),
                    "before": audit_detail_value(change.get("antes")),
                    "after": audit_detail_value(change.get("depois")),
                }
            )

    if isinstance(details, dict) and details.get("password_changed"):
        items.append({"label": "Senha", "before": "-", "after": "Alterada"})

    return items


def audit_changed_field_names(details: dict | None) -> set[str]:
    raw_changes = details.get("changes") if isinstance(details, dict) else None
    if not isinstance(raw_changes, dict):
        return set()
    return {str(field_name) for field_name in raw_changes.keys()}


def audit_change_focus_text(details: dict | None, focus_keys: set[str] | None = None) -> str:
    raw_changes = details.get("changes") if isinstance(details, dict) else None
    if not isinstance(raw_changes, dict):
        return ""

    labels: list[str] = []
    for field_name in raw_changes.keys():
        if focus_keys and field_name not in focus_keys:
            continue
        labels.append(audit_field_label(str(field_name)))

    if isinstance(details, dict) and details.get("password_changed"):
        if not focus_keys or "password" in focus_keys:
            labels.append("Senha")

    return ", ".join(dict.fromkeys(labels))


def audit_detail_items(details: dict | None) -> list[dict]:
    if not isinstance(details, dict):
        return []

    items: list[dict] = []

    motivo = details.get("motivo")
    if motivo:
        items.append({"label": "Motivo", "value": audit_reason_label(motivo)})

    if details.get("target_sector"):
        items.append({"label": audit_field_label("target_sector"), "value": audit_detail_value(details.get("target_sector"))})

    if details.get("affected_count") is not None:
        items.append({"label": audit_field_label("affected_count"), "value": audit_detail_value(details.get("affected_count"))})

    affected_members = details.get("affected_members")
    if isinstance(affected_members, list) and affected_members:
        items.append({"label": audit_field_label("affected_members"), "value": audit_detail_value(affected_members)})

    if details.get("merged_count") is not None:
        items.append({"label": audit_field_label("merged_count"), "value": audit_detail_value(details.get("merged_count"))})

    merge_items = details.get("merge_items")
    if isinstance(merge_items, list) and merge_items:
        merge_labels: list[str] = []
        for item in merge_items[:8]:
            if not isinstance(item, dict):
                continue
            duplicate_name = audit_detail_value(item.get("duplicate_member_name"))
            canonical_name = audit_detail_value(item.get("canonical_member_name"))
            merge_labels.append(f"{duplicate_name} -> {canonical_name}")
        if merge_labels:
            items.append({"label": "Fusoes", "value": " | ".join(merge_labels)})

    if details.get("canonical_member_name"):
        items.append({"label": audit_field_label("canonical_member_name"), "value": audit_detail_value(details.get("canonical_member_name"))})
    if details.get("duplicate_member_name"):
        items.append({"label": audit_field_label("duplicate_member_name"), "value": audit_detail_value(details.get("duplicate_member_name"))})

    return items


def audit_endpoint_label(endpoint: str | None, request_path: str | None) -> str:
    endpoint_name = (endpoint or "").strip()
    if endpoint_name in AUDIT_ENDPOINT_LABELS:
        return AUDIT_ENDPOINT_LABELS[endpoint_name]

    path_name = (request_path or "").strip()
    if not path_name or path_name == "/":
        return "pagina inicial"

    normalized = path_name.strip("/").replace("-", " ").replace("/", " / ")
    return normalized or "rota do sistema"


def audit_access_context(details: dict | None) -> str:
    if not isinstance(details, dict):
        return ""

    payload = details.get("payload")
    query = payload.get("query") if isinstance(payload, dict) else None
    if not isinstance(query, dict):
        return ""

    parts: list[str] = []
    if query.get("sector"):
        parts.append(f"setor {query['sector']}")
    if query.get("status"):
        parts.append(f"status {query['status']}")
    if query.get("category"):
        parts.append(f"categoria {query['category']}")
    if query.get("event_type"):
        parts.append(f"tipo {query['event_type']}")
    if query.get("actor"):
        parts.append(f"usuario {query['actor']}")
    if query.get("action"):
        parts.append(f"acao {query['action']}")
    if query.get("q"):
        parts.append(f"pesquisa \"{query['q']}\"")

    return ", ".join(parts)


def audit_reason_label(reason_code) -> str:
    reason = audit_detail_value(reason_code)
    return AUDIT_REASON_LABELS.get(reason, reason)


def summarize_audit_log(row: dict, details: dict | None) -> tuple[str, str]:
    action = row.get("action")
    target_label = row.get("target_label")
    request_path = row.get("request_path") or row.get("endpoint") or "sistema"
    action_label = audit_action_label(action)
    endpoint = row.get("endpoint")
    request_method = (row.get("request_method") or "").upper()
    page_label = audit_endpoint_label(endpoint, row.get("request_path"))

    if row.get("event_type") == "acesso" and request_method == "GET":
        payload = details.get("payload") if isinstance(details, dict) else None
        query = payload.get("query") if isinstance(payload, dict) else None
        if (endpoint or "").strip() == "relatorio_financeiro" and isinstance(query, dict) and str(query.get("format", "")).lower() == "csv":
            context = audit_access_context(details)
            return ("Exportou relatorio financeiro em CSV", context)
        context = audit_access_context(details)
        return (f"Abriu {page_label}", context)
    if row.get("event_type") == "acesso" and request_method == "POST":
        operation_label = AUDIT_POST_OPERATION_LABELS.get((endpoint or "").strip(), page_label)
        return (f"Enviou {operation_label}", "")

    if action == "acessou_página":
        return (f"Acessou {request_path}", "")
    if action == "enviou_formulário":
        return (f"Enviou formulario em {request_path}", "")
    if action == "acessou_pagina":
        return (f"Acessou {request_path}", "")
    if action == "enviou_formulario":
        return (f"Enviou formulario em {request_path}", "")
    if action == "usuario_atualizado":
        changed_fields_set = audit_changed_field_names(details)
        permission_keys = {"role", "role_label", "status"}
        account_keys = {"login_username"}
        if changed_fields_set & permission_keys:
            return (
                f"Alterou permissoes de {target_label or 'usuario'}",
                audit_change_focus_text(details, permission_keys),
            )
        if (isinstance(details, dict) and details.get("password_changed")) or changed_fields_set & account_keys:
            return (
                f"Atualizou credenciais de {target_label or 'usuario'}",
                audit_change_focus_text(details, account_keys | {"password"}),
            )
        return (
            f"Atualizou cadastro de acesso de {target_label or 'usuario'}",
            audit_change_focus_text(details),
        )
    if action == "membro_atualizado":
        changed_fields_set = audit_changed_field_names(details)
        if changed_fields_set & {"sectors", "unit"}:
            return (
                f"Alterou lotacao de {target_label or 'membro'}",
                audit_change_focus_text(details, {"unit", "sectors"}),
            )
        if changed_fields_set & {"role", "status", "rank"}:
            return (
                f"Atualizou dados funcionais de {target_label or 'membro'}",
                audit_change_focus_text(details, {"rank", "role", "status"}),
            )
        return (
            f"Atualizou cadastro de {target_label or 'membro'}",
            audit_change_focus_text(details),
        )
    if action == "membros_marcados_sem_setor_adm":
        affected_count = audit_detail_value((details or {}).get("affected_count"))
        return ("Marcou membros sem setor ADM", f"{affected_count} registro(s) afetado(s)")
    if action == "duplicados_unificados":
        merged_count = audit_detail_value((details or {}).get("merged_count"))
        return ("Unificou membros duplicados", f"{merged_count} fusao(oes)")
    if action == "membros_acoplados":
        duplicate_name = audit_detail_value((details or {}).get("duplicate_member_name"))
        canonical_name = audit_detail_value((details or {}).get("canonical_member_name") or target_label)
        return (f"Acoplou cadastro em {canonical_name}", duplicate_name)
    if action == "vinculos_financeiros_sincronizados":
        affected_count = audit_detail_value((details or {}).get("affected_count"))
        return ("Sincronizou vinculos do financeiro", f"{affected_count} registro(s) afetado(s)")
    if action == "logout":
        return ("Saiu do sistema", "")
    if action == "login_local_sucesso":
        return ("Entrou no sistema", "")
    if action == "login_discord_sucesso":
        return ("Entrou no sistema via Discord", "")
    if action in {"login_local_falhou", "login_local_negado", "login_discord_negado"}:
        reason = audit_reason_label((details or {}).get("motivo"))
        return (action_label, reason if reason != "-" else "")
    if action == "oauth_discord_iniciado":
        return ("Iniciou autenticacao via Discord", "")
    if action in {"oauth_discord_indisponivel", "oauth_discord_falhou"}:
        reason = audit_reason_label((details or {}).get("motivo"))
        return ("Falha na autenticacao via Discord", reason if reason != "-" else "")
    if target_label:
        return (f"{action_label}: {target_label}", "")
    if row.get("target_type") and row.get("target_id"):
        return (f"{action_label}: {row['target_type']} #{row['target_id']}", "")
    return (action_label, "")


def log_audit_event(
    event_type: str,
    action: str,
    *,
    actor: dict | None = None,
    target_type: str | None = None,
    target_id: str | int | None = None,
    target_label: str | None = None,
    details: dict | None = None,
    endpoint: str | None = None,
    request_path: str | None = None,
    request_method: str | None = None,
    response_status: int | None = None,
) -> None:
    try:
        ensure_audit_schema()
        actor_data = audit_actor_snapshot(actor)
        serialized_details = None
        if details:
            serialized_details = json.dumps(audit_json_value(details), ensure_ascii=False, sort_keys=True)
        execute(
            """
            insert into audit_logs (
                event_type,
                action,
                endpoint,
                request_path,
                request_method,
                response_status,
                actor_system_user_id,
                actor_username,
                actor_display_name,
                actor_role,
                actor_auth_provider,
                target_type,
                target_id,
                target_label,
                ip_address,
                user_agent,
                details_json,
                created_at
            ) values (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
            )
            """,
            (
                event_type,
                action,
                endpoint or request.endpoint,
                request_path or request.path,
                request_method or request.method,
                response_status,
                actor_data.get("system_user_id"),
                actor_data.get("username"),
                actor_data.get("display_name"),
                actor_data.get("role"),
                actor_data.get("auth_provider"),
                target_type,
                str(target_id) if target_id is not None else None,
                target_label,
                client_ip_address(),
                (request.user_agent.string or "")[:255],
                serialized_details,
            ),
        )
    except Exception:
        return


@app.context_processor
def inject_auth_context() -> dict:
    current_user = current_auth_user()
    return {
        "static_asset_version": app.config.get("STATIC_ASSET_VERSION", "1"),
        "discord_auth_enabled": discord_auth_enabled(),
        "current_user": current_user,
        "current_user_is_admin": current_user_is_admin(),
        "current_user_is_developer": current_user_is_developer(),
        "current_user_can_manage_access": current_user_can_manage_access(),
        "current_user_can_create_access": current_user_can_create_access(),
        "current_user_can_view_audit": current_user_can_view_audit(),
        "current_user_can_edit": current_user_can_edit(),
        "current_user_role_label": display_role_label(
            current_user.get("role"),
            current_user.get("auth_provider"),
            current_user.get("role_label"),
        ) if current_user else "",
    }


@app.before_request
def prepare_audit_context():
    g.audit_actor_before = audit_actor_snapshot()
    g.audit_skip_request_log = request.endpoint in {"static", "live_revision"} or request.method == "OPTIONS"
    if not g.audit_skip_request_log and not current_auth_user():
        g.audit_skip_request_log = True


@app.after_request
def record_request_audit(response: Response) -> Response:
    if getattr(g, "audit_skip_request_log", False):
        return response

    actor = current_auth_user() or getattr(g, "audit_actor_before", {}) or {}
    event_type = "acesso"
    action = "requisição"
    if request.method == "GET":
        action = "acessou_página"
    elif request.method == "POST":
        action = "enviou_formulário"

    log_audit_event(
        event_type,
        action,
        actor=actor,
        response_status=response.status_code,
        details={
            "host": request.host,
            "payload": request_payload_snapshot(),
        },
    )
    return response


@app.before_request
def enforce_public_base_url():
    # Canonicalizacao de dominio/HTTPS fica no Nginx. Fazer isso tambem no
    # Flask atras do proxy pode criar loop de redirect em /login e callbacks.
    return None


@app.before_request
def require_app_login():
    public_endpoints = {
        "login",
        "login_password",
        "discord_login",
        "discord_callback",
        "logout",
        "static",
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
                or active_user.get("role_label") != system_user.get("role_label")
                or active_user.get("display_name") != system_user.get("display_name")
            ):
                session["auth_user"] = {
                    **active_user,
                    "display_name": system_user.get("display_name") or active_user.get("display_name"),
                    "role": system_user.get("role"),
                    "role_label": system_user.get("role_label"),
                    "status": system_user.get("status"),
                    "system_user_id": system_user.get("id"),
                }
            return None

        session.clear()
        return redirect(url_for("login", error="blocked"))

    return redirect(url_for("login", next=request.full_path.rstrip("?")))


def require_admin_access() -> Response | None:
    if not current_user_can_manage_access():
        return redirect(url_for("dashboard"))
    return None


def require_developer_access() -> Response | None:
    if not current_user_is_developer():
        return redirect(url_for("dashboard"))
    return None


def require_write_access() -> Response | tuple[dict, int] | None:
    if current_user_can_edit():
        return None

    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return {"ok": False, "error": "forbidden"}, 403
    return redirect(url_for("dashboard"))


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


def execute_rowcount(query: str, params: tuple = ()) -> int:
    with db() as connection:
        cursor = connection.cursor()
        cursor.execute(query, params)
        affected_rows = cursor.rowcount
        connection.commit()
        return affected_rows


def execute_transaction(statements: list[tuple[str, tuple]]) -> None:
    with db() as connection:
        cursor = connection.cursor()
        for query, params in statements:
            cursor.execute(query, params)
        connection.commit()


def latest_data_change_marker() -> str:
    tracked_queries = [
        ("select max(coalesce(updated_at, created_at, last_login_at)) as changed_at from system_users", ()),
        ("select max(created_at) as changed_at from audit_logs", ()),
        ("select max(coalesce(updated_at, created_at)) as changed_at from sectors", ()),
        ("select max(coalesce(updated_at, created_at)) as changed_at from members", ()),
        ("select max(created_at) as changed_at from member_sectors", ()),
        ("select max(coalesce(updated_at, created_at, paid_at)) as changed_at from financial_payments", ()),
        ("select max(created_at) as changed_at from financial_import_batches", ()),
        ("select max(coalesce(updated_at, created_at, submitted_at)) as changed_at from department_applications", ()),
    ]

    latest_change: datetime | None = None
    for query, params in tracked_queries:
        try:
            row = fetch_one(query, params)
        except mysql.connector.Error:
            continue

        changed_at = row.get("changed_at") if row else None
        if changed_at and (latest_change is None or changed_at > latest_change):
            latest_change = changed_at

    return latest_change.isoformat() if latest_change else "0"


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


def parse_reference_month_value(raw_value: str | None) -> date:
    value = (raw_value or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}", value):
        raise ValueError("reference_month")
    parsed = datetime.strptime(f"{value}-01", "%Y-%m-%d").date()
    if parsed < FINANCIAL_REFERENCE_MIN or parsed > FINANCIAL_REFERENCE_MAX:
        raise ValueError("reference_month")
    return parsed


def reference_month_key(reference_month: date | None) -> str:
    if not reference_month:
        return ""
    return reference_month.strftime("%Y-%m")


def normalize_reference_month_arg(raw_value: str | None) -> str:
    value = (raw_value or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}", value):
        return ""
    try:
        parsed = datetime.strptime(f"{value}-01", "%Y-%m-%d").date()
    except ValueError:
        return ""
    if parsed < FINANCIAL_REFERENCE_MIN or parsed > FINANCIAL_REFERENCE_MAX:
        return ""
    return value


def allowed_reference_month_keys() -> list[str]:
    return [f"2026-{month:02d}" for month in range(3, 13)]


def normalize_backup_rank(raw_rank: str | None) -> str:
    raw = (raw_rank or "").strip()
    if not raw:
        return "Outros"

    text = unicodedata.normalize("NFKD", raw)
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    key = re.sub(r"\s+", " ", text).strip()
    mapping = {
        "t coronel": "T.Coronel",
        "coronel": "Coronel",
        "major": "Major",
        "capitao": "Capitao",
        "2 tenente": "2.Tenente",
        "1 tenente": "1.Tenente",
        "s tenente": "S.Tenente",
        "aspirante": "Aspirante",
        "1 sargento": "1.Sargento",
        "2 sargento": "2.Sargento",
        "3 sargento": "3.Sargento",
        "cabo": "Cabo",
        "sd 1a cl": "Sd 1a Cl",
    }
    return mapping.get(key, raw)


def rank_from_name(name: str) -> str:
    normalized_name = normalized_identity(name)
    rank_rules = [
        ("t coronel", "T.Coronel"),
        ("coronel", "Coronel"),
        ("capit", "Capitao"),
        ("1 tenente", "1.Tenente"),
        ("2 tenente", "2.Tenente"),
        ("s tenente", "S.Tenente"),
        ("aspirante", "Aspirante"),
        ("1 sargento", "1.Sargento"),
        ("2 sargento", "2.Sargento"),
        ("3 sargento", "3.Sargento"),
        ("cabo", "Cabo"),
        ("sd 1", "Sd 1a Cl"),
    ]
    for needle, rank_name in rank_rules:
        if needle in normalized_name:
            return rank_name
    return "Outros"


def parse_import_money(raw_value: str) -> Decimal:
    cleaned = (raw_value or "").replace("R$", "").replace(" ", "")
    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif cleaned.count(".") == 1 and len(cleaned.rsplit(".", 1)[1]) <= 2:
        cleaned = cleaned
    else:
        cleaned = cleaned.replace(".", "")
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        raise ValueError("amount") from None


def parse_import_minutes(raw_value: str | None) -> int | None:
    value = (raw_value or "").strip()
    if not value:
        return None
    if ":" in value:
        hour, minute = value.split(":", 1)
        return (int(hour) * 60) + int(minute)
    return int(value)


def split_admin_pending_member(raw_name: str) -> tuple[str, str | None, int | None]:
    cleaned = (raw_name or "").strip()
    match = ADMIN_PENDING_MEMBER_PATTERN.match(cleaned)
    if not match:
        return cleaned, None, None
    name, registration_number, function_count = match.groups()
    normalized_name = re.sub(r"\s*\([^)]*\)\s*$", "", (name or "").strip()).strip()
    return (
        normalized_name,
        (registration_number or "").strip() or None,
        int(function_count) if function_count else None,
    )


def canonical_import_member_name(raw_name: str | None) -> str:
    base_name = (raw_name or "").strip()
    if not base_name:
        return ""
    normalized_base = normalized_identity(base_name)
    for source_name, target_name in IMPORT_MEMBER_NAME_ALIASES.items():
        if normalized_identity(source_name) == normalized_base:
            return target_name.strip()
    return base_name


def parse_financial_entries_from_structured_text(raw_text: str) -> list[dict]:
    entries: list[dict] = []
    section = "Horas"
    pending_admin_name: str | None = None
    pending_admin_function_count: int | None = None
    pending_admin_functions_label: str | None = None
    current_admin_functions_label: str | None = None

    for raw_line in raw_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            line_lower = line.lower()
            if "administrativo" in line_lower or "setor" in line_lower:
                section = "Administrativo"
            elif "hora" in line_lower:
                section = "Horas"
            continue

        if set(line) <= {"=", "-"}:
            continue

        if line.upper().startswith(
            (
                "PAGAMENTO ",
                "TOTAL",
                "VALOR TOTAL",
                "DEPARTAMENTO ",
                "CORREGEDORIA",
                "COMANDO ",
                "COORDENADOR ",
                "DIRETOR ",
                "CHEFE ",
                "AUXILIAR ",
                "ADMINISTRATIVO ",
                "INSTRUTOR ",
            )
        ):
            if line.upper().startswith(
                (
                    "CORREGEDORIA",
                    "COMANDO ",
                    "COORDENADOR ",
                    "DIRETOR ",
                    "CHEFE ",
                    "AUXILIAR ",
                    "ADMINISTRATIVO ",
                    "INSTRUTOR ",
                )
            ):
                current_admin_functions_label = line.strip()
                section = "Administrativo"
            continue

        value_match = ADMIN_VALUE_PATTERN.match(line)
        if value_match and pending_admin_name:
            pending_name, pending_registration, inline_function_count = split_admin_pending_member(pending_admin_name)
            entries.append(
                {
                    "category": "Administrativo",
                    "name": pending_name,
                    "rank": rank_from_name(pending_name),
                    "amount": parse_import_money(value_match.group(1)),
                    "department": None,
                    "registration_number": pending_registration,
                    "total_minutes": None,
                    "extra_minutes": None,
                    "function_count": pending_admin_function_count or inline_function_count,
                    "functions_label": pending_admin_functions_label or current_admin_functions_label,
                }
            )
            pending_admin_name = None
            pending_admin_function_count = None
            pending_admin_functions_label = None
            continue

        if line.startswith("* @") or line.startswith("@"):
            bullet_match = ADMIN_BULLET_PATTERN.match(line)
            if bullet_match:
                pending_admin_name = bullet_match.group(1).strip()
                pending_admin_function_count = int(bullet_match.group(3))
                pending_admin_functions_label = "INSTRUTOR"
                section = "Administrativo"
                continue

        if not line.startswith("@"):
            if "Valor:" not in line:
                pending_admin_name = line
                pending_admin_function_count = None
                pending_admin_functions_label = None
                section = "Administrativo"
            continue

        if section == "Administrativo":
            admin_match = ADMIN_IMPORT_PATTERN.match(line)
            if not admin_match:
                continue
            name, function_count, functions_label, amount = admin_match.groups()
            entries.append(
                {
                    "category": "Administrativo",
                    "name": name.strip(),
                    "rank": rank_from_name(name),
                    "amount": parse_import_money(amount),
                    "department": None,
                    "registration_number": None,
                    "total_minutes": None,
                    "extra_minutes": None,
                    "function_count": int(function_count),
                    "functions_label": " + ".join(part.strip() for part in functions_label.split("+")),
                }
            )
            continue

        hourly_match = HOURLY_IMPORT_WITH_ID_PATTERN.match(line) or HOURLY_IMPORT_PATTERN.match(line)
        if not hourly_match:
            continue
        if HOURLY_IMPORT_WITH_ID_PATTERN.match(line):
            name, registration_number, total_hour, total_min, extra_hour, extra_min, amount = hourly_match.groups()
        else:
            name, total_hour, total_min, extra_hour, extra_min, amount = hourly_match.groups()
            registration_number = None
        entries.append(
            {
                "category": "Horas",
                "name": name.strip(),
                "rank": rank_from_name(name),
                "amount": parse_import_money(amount),
                "department": None,
                "registration_number": (registration_number or "").strip() or None,
                "total_minutes": (int(total_hour) * 60) + int(total_min),
                "extra_minutes": (int(extra_hour) * 60) + int(extra_min) if extra_hour and extra_min else 0,
                "function_count": None,
                "functions_label": None,
            }
        )

    return entries


def parse_financial_entries_from_csv(raw_text: str) -> list[dict]:
    rows = list(csv.DictReader(StringIO(raw_text)))
    entries: list[dict] = []
    for row in rows:
        try:
            normalized = {(key or "").strip().lower(): (value or "").strip() for key, value in row.items()}
            category_raw = normalized.get("categoria") or normalized.get("category") or normalized.get("source_category")
            category = "Administrativo" if (category_raw or "").lower().startswith("adm") else "Horas"
            name = (
                normalized.get("nome")
                or normalized.get("name")
                or normalized.get("source_name")
                or normalized.get("policial")
                or ""
            ).strip()
            if not name:
                continue
            amount_raw = (
                normalized.get("valor")
                or normalized.get("amount")
                or normalized.get("gross_amount")
                or normalized.get("liquido")
                or normalized.get("net_amount")
                or ""
            )
            if not amount_raw:
                continue

            rank = normalized.get("posto") or normalized.get("rank") or rank_from_name(name)
            entries.append(
                {
                    "category": category,
                    "name": name,
                    "rank": rank,
                    "amount": parse_import_money(amount_raw),
                    "department": normalized.get("departamento") or normalized.get("department") or None,
                    "registration_number": (
                        normalized.get("matricula")
                        or normalized.get("registro")
                        or normalized.get("registration_number")
                        or None
                    ),
                    "total_minutes": parse_import_minutes(
                        normalized.get("total_horas")
                        or normalized.get("total_minutes")
                        or normalized.get("minutos_totais")
                    ),
                    "extra_minutes": parse_import_minutes(
                        normalized.get("extra_horas")
                        or normalized.get("extra_minutes")
                        or normalized.get("minutos_extras")
                    ),
                    "function_count": form_int_from_value(
                        normalized.get("qtd_funcoes")
                        or normalized.get("function_count")
                    ),
                    "functions_label": normalized.get("funcoes") or normalized.get("functions_label") or None,
                }
            )
        except ValueError:
            continue
    return entries


def form_int_from_value(raw_value: str | None) -> int | None:
    value = (raw_value or "").strip()
    if not value:
        return None
    return int(value)


def parse_financial_entries(raw_text: str) -> list[dict]:
    entries = parse_financial_entries_from_structured_text(raw_text)
    if entries:
        return entries
    return parse_financial_entries_from_csv(raw_text)


def source_key_for_entry(entry: dict, reference_month: date) -> str:
    raw = "|".join(
        [
            reference_month.strftime("%Y-%m-01"),
            entry.get("category") or "",
            normalized_identity(entry.get("name") or ""),
            str(entry.get("amount") or ""),
            str(entry.get("total_minutes") or ""),
            str(entry.get("extra_minutes") or ""),
            str(entry.get("function_count") or ""),
            normalized_identity(entry.get("functions_label") or ""),
            normalized_identity(entry.get("department") or ""),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def normalized_person_name(value: str | None) -> str:
    normalized = normalized_identity(value)
    if not normalized:
        return ""
    tokens = normalized.split()
    rank_tokens = {
        "coronel",
        "tenente",
        "sargento",
        "capitao",
        "cabo",
        "aspirante",
        "major",
        "soldado",
        "classe",
        "cl",
        "sd",
        "t",
        "s",
        "1",
        "2",
        "3",
        "1a",
    }
    filtered = [token for token in tokens if token not in rank_tokens]
    return " ".join(filtered).strip()


def person_tokens(value: str | None) -> list[str]:
    person = normalized_person_name(value)
    return [token for token in person.split() if token]


def names_probably_same_person(left_name: str | None, right_name: str | None) -> bool:
    left_tokens = person_tokens(left_name)
    right_tokens = person_tokens(right_name)
    if not left_tokens or not right_tokens:
        return False
    if left_tokens == right_tokens:
        return True

    left_last = left_tokens[-1]
    right_last = right_tokens[-1]
    if left_last != right_last:
        return False

    left_initial = left_tokens[0][0] if left_tokens[0] else ""
    right_initial = right_tokens[0][0] if right_tokens[0] else ""
    if left_initial and right_initial and left_initial == right_initial:
        return True

    overlap = set(left_tokens) & set(right_tokens)
    return len(overlap) >= 2


def find_member_for_import(cursor, entry: dict) -> int | None:
    registration_number = (entry.get("registration_number") or "").strip()
    if registration_number:
        cursor.execute("select id from members where registration_number = %s", (registration_number,))
        existing_by_registration = cursor.fetchone()
        if existing_by_registration:
            return int(existing_by_registration[0])

    cursor.execute("select id, full_name, `rank` from members")
    raw_name = re.sub(r"\s*-\s*\d+\s*$", "", str(entry.get("name") or "")).strip()
    raw_name = re.sub(r"\s*\(\*\*\d+\*\*\)\s*$", "", raw_name).strip()
    raw_name = re.sub(r"\s*\([^)]*\)\s*$", "", raw_name).strip()
    raw_name = canonical_import_member_name(raw_name)
    target_name = normalized_identity(raw_name)
    target_person = normalized_person_name(raw_name)
    target_rank = entry.get("rank")
    fallback_candidates: list[tuple[int, str | None]] = []
    fuzzy_candidates: list[tuple[int, str | None]] = []

    for member_id, full_name, rank in cursor.fetchall():
        member_full = normalized_identity(full_name)
        if member_full == target_name and ranks_are_compatible(rank, target_rank):
            return int(member_id)
        if normalized_person_name(full_name) == target_person and target_person:
            fallback_candidates.append((int(member_id), rank))
        elif target_person and names_probably_same_person(raw_name, full_name):
            fuzzy_candidates.append((int(member_id), rank))

    if len(fallback_candidates) == 1:
        return fallback_candidates[0][0]

    rank_compatible = [member_id for member_id, rank in fallback_candidates if ranks_are_compatible(rank, target_rank)]
    if len(rank_compatible) == 1:
        return rank_compatible[0]

    fuzzy_rank_compatible = [member_id for member_id, rank in fuzzy_candidates if ranks_are_compatible(rank, target_rank)]
    if len(fuzzy_rank_compatible) == 1:
        return fuzzy_rank_compatible[0]

    if len(fuzzy_candidates) == 1:
        return fuzzy_candidates[0][0]

    return None


def infer_department_for_member(cursor, member_id: int) -> str | None:
    cursor.execute(
        """
        select s.name
        from member_sectors ms
        inner join sectors s on s.id = ms.sector_id
        where ms.member_id = %s
        order by s.name asc
        limit 1
        """,
        (member_id,),
    )
    sector_row = cursor.fetchone()
    if sector_row and sector_row[0]:
        return str(sector_row[0])

    cursor.execute("select unit from members where id = %s", (member_id,))
    member_row = cursor.fetchone()
    if not member_row or not member_row[0]:
        return None
    unit = str(member_row[0]).strip()
    if not unit or unit.upper() == NO_ADMIN_SECTOR:
        return None
    return unit


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


def ensure_financial_import_schema() -> None:
    execute(
        """
        create table if not exists financial_import_batches (
            id bigint unsigned not null auto_increment primary key,
            reference_month date not null,
            input_mode varchar(20) not null,
            source_name varchar(255) null,
            source_preview text null,
            total_entries int unsigned not null default 0,
            created_members int unsigned not null default 0,
            created_payments int unsigned not null default 0,
            updated_payments int unsigned not null default 0,
            actor_system_user_id bigint unsigned null,
            created_at timestamp not null default current_timestamp,
            index financial_import_batches_reference_month_idx (reference_month),
            index financial_import_batches_created_at_idx (created_at desc),
            constraint financial_import_batches_actor_fk
                foreign key (actor_system_user_id) references system_users(id)
                on delete set null
        ) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci
        """
    )


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
    affected_rows = execute_rowcount(
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
    if affected_rows > 0:
        log_audit_event(
            "financeiro",
            "vinculos_financeiros_sincronizados",
            details={"affected_count": affected_rows},
        )


def member_sector_names(member_id: int) -> list[str]:
    rows = fetch_all(
        """
        select s.name
        from member_sectors ms
        inner join sectors s on s.id = ms.sector_id
        where ms.member_id = %s
        order by s.name asc
        """,
        (member_id,),
    )
    return [row["name"] for row in rows if row.get("name")]


def merge_duplicate_members_by_identity() -> dict:
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
    merge_items: list[dict] = []

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
            merge_items.append(
                {
                    "canonical_member_id": canonical_id,
                    "canonical_member_name": canonical["full_name"],
                    "duplicate_member_id": duplicate_id,
                    "duplicate_member_name": member["full_name"],
                    "registration_number": member["registration_number"],
                }
            )

    if statements:
        execute_transaction(statements)

    return {"merged_count": merged_count, "items": merge_items}


def merge_member_records(canonical_id: int, duplicate_id: int) -> dict | None:
    if canonical_id == duplicate_id:
        return None

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
        return None

    canonical_sectors = member_sector_names(canonical_id)
    duplicate_sectors = member_sector_names(duplicate_id)

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
    merged_sectors = sorted(set(canonical_sectors) | set(duplicate_sectors))
    return {
        "canonical_member_id": canonical_id,
        "canonical_member_name": canonical["full_name"],
        "duplicate_member_id": duplicate_id,
        "duplicate_member_name": duplicate["full_name"],
        "changes": changed_fields(
            {
                "unit": canonical["unit"],
                "role": canonical["role"],
                "sectors": canonical_sectors,
            },
            {
                "unit": canonical_unit,
                "role": canonical_role,
                "sectors": merged_sectors,
            },
        ),
    }


@app.get("/login")
def login():
    active_user = current_auth_user()
    if active_user:
        system_user = access_user_by_id(active_user.get("system_user_id"))
        if system_user and system_user.get("status") == "ativo":
            return redirect(url_for("dashboard"))
        session.clear()

    return render_login_page()


@app.post("/login")
def login_password():
    ensure_auth_schema()
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")
    next_target = safe_redirect_target(request.form.get("next"))
    if not username or not password:
        log_audit_event(
            "autenticacao",
            "login_local_falhou",
            target_type="usuario",
            target_label=username or "sem_usuario",
            details={"motivo": "credenciais_ausentes", "next": next_target},
        )
        return render_login_page(error="local_missing", next_target=next_target), 400

    system_user = access_user_for_login_username(username)
    if not system_user or not system_user.get("password_hash"):
        log_audit_event(
            "autenticacao",
            "login_local_falhou",
            target_type="usuario",
            target_label=username,
            details={"motivo": "usuario_invalido", "next": next_target},
        )
        return render_login_page(error="local_invalid", next_target=next_target), 401

    if not check_password_hash(system_user["password_hash"], password):
        log_audit_event(
            "autenticacao",
            "login_local_falhou",
            target_type="usuario",
            target_id=system_user.get("id"),
            target_label=username,
            details={"motivo": "senha_invalida", "next": next_target},
        )
        return render_login_page(error="local_invalid", next_target=next_target), 401

    if system_user.get("status") != "ativo":
        error_code = "pending" if system_user.get("status") == "pendente" else "blocked"
        log_audit_event(
            "autenticacao",
            "login_local_negado",
            target_type="usuario",
            target_id=system_user.get("id"),
            target_label=username,
            details={"motivo": error_code, "next": next_target},
        )
        return render_login_page(error=error_code, next_target=next_target), 403

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
    log_audit_event(
        "autenticacao",
        "login_local_sucesso",
        actor=session["auth_user"],
        target_type="usuario",
        target_id=system_user.get("id"),
        target_label=system_user.get("display_name") or username,
        details={"next": next_target},
    )
    return redirect(next_target)


@app.get("/login/discord")
def discord_login():
    if not discord_auth_enabled():
        log_audit_event("autenticacao", "oauth_discord_indisponivel", details={"motivo": "configuracao_ausente"})
        return redirect(url_for("login", error="config"))

    state = secrets.token_urlsafe(24)
    next_target = safe_redirect_target(request.args.get("next"))
    redirect_uri = discord_redirect_uri()
    session["discord_oauth_state"] = state
    session["discord_oauth_redirect_uri"] = redirect_uri
    session["post_login_redirect"] = next_target
    log_audit_event(
        "autenticacao",
        "oauth_discord_iniciado",
        details={"redirect_uri": redirect_uri, "next": next_target},
    )
    authorize_query = urlencode(
        {
            "client_id": app.config["DISCORD_CLIENT_ID"],
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": "identify",
            "state": state,
        }
    )
    return redirect(f"{DISCORD_AUTHORIZE_URL}?{authorize_query}")


@app.get("/auth/discord/callback")
def discord_callback():
    if not discord_auth_enabled():
        log_audit_event("autenticacao", "oauth_discord_indisponivel", details={"motivo": "configuracao_ausente"})
        return redirect(url_for("login", error="config"))

    expected_state = session.get("discord_oauth_state")
    received_state = request.args.get("state", "")
    if not expected_state or received_state != expected_state:
        session.pop("discord_oauth_state", None)
        session.pop("discord_oauth_redirect_uri", None)
        log_audit_event("autenticacao", "oauth_discord_falhou", details={"motivo": "state_invalido"})
        return redirect(url_for("login", error="state"))

    if request.args.get("error"):
        session.pop("discord_oauth_state", None)
        session.pop("discord_oauth_redirect_uri", None)
        session.pop("post_login_redirect", None)
        log_audit_event(
            "autenticacao",
            "oauth_discord_falhou",
            details={"motivo": "acesso_negado", "error": request.args.get("error", "")},
        )
        return redirect(url_for("login", error="denied"))

    code = request.args.get("code", "")
    if not code:
        session.pop("discord_oauth_state", None)
        session.pop("discord_oauth_redirect_uri", None)
        session.pop("post_login_redirect", None)
        log_audit_event("autenticacao", "oauth_discord_falhou", details={"motivo": "codigo_ausente"})
        return redirect(url_for("login", error="missing_code"))

    try:
        token_payload = exchange_discord_code(code)
        access_token = token_payload["access_token"]
        user = fetch_discord_user(access_token)
    except HTTPError as error:
        session.pop("discord_oauth_state", None)
        session.pop("discord_oauth_redirect_uri", None)
        session.pop("post_login_redirect", None)
        payload = extract_http_error_payload(error)
        detail = (
            payload.get("error_description")
            or payload.get("error")
            or payload.get("detail")
            or f"HTTP {error.code}"
        )
        log_audit_event(
            "autenticacao",
            "oauth_discord_falhou",
            details={"motivo": "http_error", "detail": detail},
        )
        return redirect(url_for("login", error="oauth", detail=detail))
    except (KeyError, URLError, TimeoutError, json.JSONDecodeError):
        session.pop("discord_oauth_state", None)
        session.pop("discord_oauth_redirect_uri", None)
        session.pop("post_login_redirect", None)
        log_audit_event("autenticacao", "oauth_discord_falhou", details={"motivo": "resposta_invalida"})
        return redirect(url_for("login", error="oauth"))

    discord_id = user.get("id")
    system_user = sync_system_user(user)
    if system_user.get("status") != "ativo":
        session.clear()
        error_code = "pending" if system_user.get("status") == "pendente" else "blocked"
        log_audit_event(
            "autenticacao",
            "login_discord_negado",
            target_type="usuario",
            target_id=system_user.get("id"),
            target_label=system_user.get("display_name"),
            details={"motivo": error_code, "discord_id": discord_id},
        )
        return redirect(url_for("login", error=error_code))

    session.pop("discord_oauth_state", None)
    session.pop("discord_oauth_redirect_uri", None)
    redirect_target = safe_redirect_target(session.pop("post_login_redirect", None))
    session.permanent = True
    session["auth_user"] = build_session_user(
        system_user,
        auth_provider="discord",
        avatar_url=discord_avatar_url(user),
    )
    log_audit_event(
        "autenticacao",
        "login_discord_sucesso",
        actor=session["auth_user"],
        target_type="usuario",
        target_id=system_user.get("id"),
        target_label=system_user.get("display_name"),
        details={"discord_id": discord_id, "next": redirect_target},
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
    actor = current_auth_user()
    if actor:
        log_audit_event(
            "autenticacao",
            "logout",
            actor=actor,
            target_type="usuario",
            target_id=actor.get("system_user_id"),
            target_label=actor.get("display_name") or actor.get("username"),
        )
    session.clear()
    return redirect(url_for("login"))


@app.get("/acessos")
def acessos():
    denied = require_developer_access()
    if denied:
        return denied

    ensure_auth_schema()
    discord_requests = fetch_all(
        """
        select *
        from system_users
        where discord_id not like 'local-%'
        order by
            field(status, 'pendente', 'bloqueado', 'ativo'),
            updated_at desc,
            id desc
        """
    )
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
        discord_requests=discord_requests,
        users=users,
        metrics=metrics,
        saved=request.args.get("saved"),
        error=request.args.get("error"),
    )


@app.post("/acessos")
def criar_acesso():
    denied = require_developer_access()
    if denied:
        return denied

    ensure_auth_schema()
    try:
        display_name = form_text("display_name", required=True)
        login_username = form_text("login_username", required=True)
        password = request.form.get("password", "").strip()
        role = form_text("role", required=True)
        role_label = form_text("role_label")
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
                (discord_id, discord_username, discord_global_name, display_name, login_username, password_hash, role, role_label, status, notes, created_at, updated_at)
            values
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
            """,
            (
                f"local-{login_username}",
                login_username,
                None,
                display_name,
                login_username,
                generate_password_hash(password),
                role,
                role_label,
                status,
                notes,
            ),
        )
    except IntegrityError:
        return redirect(url_for("acessos", error="duplicate"))

    created_user = access_user_for_login_username(login_username)
    log_audit_event(
        "administracao",
        "usuario_criado",
        target_type="usuario",
        target_id=created_user.get("id"),
        target_label=display_name,
        details={
            "login_username": login_username,
            "role": role,
            "role_label": role_label,
            "status": status,
            "notes": notes,
        },
    )
    return redirect(url_for("acessos", saved="created"))


@app.post("/acessos/<int:user_id>")
def atualizar_acesso(user_id: int):
    denied = require_developer_access()
    if denied:
        return denied

    ensure_auth_schema()
    is_autosave = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    try:
        display_name = form_text("display_name", required=True)
        login_username = form_text("login_username")
        new_password = request.form.get("new_password", "").strip()
        role = form_text("role", required=True)
        role_label = form_text("role_label")
        status = form_text("status", required=True)
        notes = form_text("notes")
    except ValueError:
        if is_autosave:
            return {"ok": False, "error": "missing"}, 400
        return redirect(url_for("acessos"))
    previous_user = fetch_one(
        """
        select id, display_name, login_username, role, role_label, status, notes
        from system_users
        where id = %s
        """,
        (user_id,),
    )
    statements: list[tuple[str, tuple]] = [
        (
            """
            update system_users
            set display_name = %s,
                login_username = %s,
                role = %s,
                role_label = %s,
                status = %s,
                notes = %s,
                updated_at = now()
            where id = %s
            """,
            (display_name, login_username, role, role_label, status, notes, user_id),
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
        if is_autosave:
            return {"ok": False, "error": "duplicate"}, 409
        return redirect(url_for("acessos", error="duplicate"))

    updated_user = access_user_by_id(user_id)
    active_user = current_auth_user()
    current_user_payload = None
    if active_user and active_user.get("system_user_id") == user_id:
        refreshed_user = build_session_user(
            updated_user,
            auth_provider=active_user.get("auth_provider") or "local",
            avatar_url=active_user.get("avatar_url"),
        )
        session["auth_user"] = refreshed_user
        current_user_payload = {
            "display_name": refreshed_user.get("display_name") or refreshed_user.get("username"),
            "role_label": display_role_label(
                refreshed_user.get("role"),
                refreshed_user.get("auth_provider"),
                refreshed_user.get("role_label"),
            ),
            "username": refreshed_user.get("username"),
        }

    log_audit_event(
        "administracao",
        "usuario_atualizado",
        target_type="usuario",
        target_id=user_id,
        target_label=display_name,
        details={
            "changes": changed_fields(
                previous_user,
                {
                    "display_name": display_name,
                    "login_username": login_username,
                    "role": role,
                    "role_label": role_label,
                    "status": status,
                    "notes": notes,
                },
            ),
            "password_changed": bool(new_password),
        },
    )
    if is_autosave:
        return {
            "ok": True,
            "saved": True,
            "password_changed": bool(new_password),
            "status": status,
            "record": {
                "display_name": updated_user.get("display_name") or display_name,
                "role_label": display_role_label(
                    updated_user.get("role"),
                    active_user.get("auth_provider") if active_user else None,
                    updated_user.get("role_label"),
                ),
            },
            "current_user": current_user_payload,
        }
    return redirect(url_for("acessos", saved="1"))


@app.get("/auditoria")
def auditoria():
    denied = require_developer_access()
    if denied:
        return denied

    ensure_audit_schema()
    query = request.args.get("q", "").strip()
    event_type = request.args.get("event_type", "").strip()
    action_filter = request.args.get("action", "").strip()
    actor_filter = request.args.get("actor", "").strip()
    try:
        limit = max(50, min(int(request.args.get("limit", "200") or "200"), 500))
    except ValueError:
        limit = 200

    where = []
    params: list = []
    if query:
        where.append(
            """
            (
                actor_display_name like %s
                or actor_username like %s
                or target_label like %s
                or request_path like %s
                or endpoint like %s
                or details_json like %s
            )
            """
        )
        like = f"%{query}%"
        params.extend([like, like, like, like, like, like])
    if event_type:
        where.append("event_type = %s")
        params.append(event_type)
    if action_filter:
        where.append("action = %s")
        params.append(action_filter)
    if actor_filter:
        where.append("(actor_display_name like %s or actor_username like %s)")
        like_actor = f"%{actor_filter}%"
        params.extend([like_actor, like_actor])

    where_sql = "where " + " and ".join(where) if where else ""
    logs = fetch_all(
        f"""
        select *
        from audit_logs
        {where_sql}
        order by created_at desc, id desc
        limit %s
        """,
        tuple(params + [limit]),
    )
    for row in logs:
        details_data: dict | None = None
        details_json = row.get("details_json")
        if details_json:
            try:
                parsed_details = json.loads(details_json)
                if isinstance(parsed_details, dict):
                    details_data = parsed_details
            except json.JSONDecodeError:
                details_data = None
        summary, context = summarize_audit_log(row, details_data)
        row["audit_summary"] = summary
        row["audit_context"] = context
        row["change_items"] = audit_change_items(details_data)
        row["detail_items"] = audit_detail_items(details_data)
        created_at = row.get("created_at")
        row["created_display"] = created_at.strftime("%d/%m/%Y %H:%M:%S") if created_at else "-"

    metrics = fetch_one(
        """
        select
            count(*) as total_logs,
            sum(case when created_at >= now() - interval 1 day then 1 else 0 end) as last_24h,
            sum(case when event_type = 'acesso' then 1 else 0 end) as access_logs,
            sum(case when event_type = 'autenticacao' then 1 else 0 end) as auth_logs,
            count(distinct coalesce(actor_system_user_id, 0)) as unique_actors
        from audit_logs
        """
    )
    event_types = fetch_all(
        """
        select event_type, count(*) as total
        from audit_logs
        group by event_type
        order by total desc, event_type asc
        """
    )
    actions = fetch_all(
        """
        select action, count(*) as total
        from audit_logs
        group by action
        order by total desc, action asc
        limit 30
        """
    )

    return render_template(
        "auditoria.html",
        logs=logs,
        metrics=metrics,
        event_types=event_types,
        actions=actions,
        query=query,
        event_type=event_type,
        action_filter=action_filter,
        actor_filter=actor_filter,
        limit=limit,
    )


@app.get("/api/live-revision")
def live_revision():
    ensure_auth_schema()
    ensure_audit_schema()
    try:
        ensure_sector_schema()
    except mysql.connector.Error:
        pass
    try:
        ensure_financial_schema()
    except mysql.connector.Error:
        pass
    try:
        ensure_financial_import_schema()
    except mysql.connector.Error:
        pass

    return {
        "ok": True,
        "revision": latest_data_change_marker(),
        "server_time": datetime.now().isoformat(),
    }


@app.get("/")
def home():
    return redirect(url_for("login"))


@app.get("/dashboard")
def dashboard():
    metrics = dashboard_metrics()
    top_payments = fetch_all(
        """
        select fp.*, m.rank
        from financial_payments fp
        left join members m on m.id = fp.member_id
        order by fp.net_amount desc
        limit 24
        """
    )
    recent_members = fetch_all(
        """
        select *
        from members
        order by created_at desc
        limit 24
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
    ensure_financial_import_schema()
    sync_financial_members()
    sectors = sector_options()
    query = request.args.get("q", "").strip()
    category = request.args.get("category", "").strip()
    selected_sector = request.args.get("sector", "").strip()
    status = request.args.get("status", "").strip()
    selected_reference_month = normalize_reference_month_arg(request.args.get("reference_month"))
    if not selected_reference_month:
        selected_reference_month = reference_month_key(FINANCIAL_REFERENCE_MIN)
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

    if status:
        where.append("fp.status = %s")
        params.append(status)

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

    where.append("date_format(fp.reference_month, '%Y-%m') = %s")
    params.append(selected_reference_month)

    where_sql = "where " + " and ".join(where) if where else ""
    metrics = dashboard_metrics()
    payment_overview = fetch_one(
        f"""
        select
            count(*) as total_count,
            coalesce(sum(net_amount), 0) as total_amount,
            coalesce(sum(case when fp.status = 'pendente' then 1 else 0 end), 0) as pending_count,
            coalesce(sum(case when fp.status = 'pendente' then net_amount else 0 end), 0) as pending_amount,
            coalesce(sum(case when fp.status = 'pago' then 1 else 0 end), 0) as paid_count,
            coalesce(sum(case when fp.status = 'pago' then net_amount else 0 end), 0) as paid_amount,
            coalesce(sum(case when fp.status = 'cancelado' then 1 else 0 end), 0) as cancelled_count,
            coalesce(sum(case when fp.status = 'cancelado' then net_amount else 0 end), 0) as cancelled_amount
        from financial_payments fp
        left join members m on m.id = fp.member_id
        {where_sql}
        """,
        tuple(params),
    )
    filtered_metrics = fetch_one(
        f"""
        select
            count(*) as total_count,
            coalesce(sum(fp.net_amount), 0) as total_amount,
            coalesce(sum(case when fp.source_category = 'Horas' then 1 else 0 end), 0) as hourly_count,
            coalesce(sum(case when fp.source_category = 'Horas' then fp.net_amount else 0 end), 0) as hourly_amount,
            coalesce(sum(case when fp.source_category = 'Horas' then fp.total_minutes else 0 end), 0) as total_minutes,
            coalesce(sum(case when fp.source_category = 'Horas' then fp.extra_minutes else 0 end), 0) as extra_minutes,
            coalesce(sum(case when fp.source_category = 'Administrativo' then 1 else 0 end), 0) as administrative_count,
            coalesce(sum(case when fp.source_category = 'Administrativo' then fp.net_amount else 0 end), 0) as administrative_amount
        from financial_payments fp
        left join members m on m.id = fp.member_id
        {where_sql}
        """,
        tuple(params),
    )
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
        order by
            case fp.status
                when 'pendente' then 0
                when 'pago' then 1
                when 'cancelado' then 2
                else 3
            end,
            fp.net_amount desc,
            fp.source_name asc
        """,
        tuple(params),
    )
    payment_groups = {
        "pendente": [],
        "pago": [],
        "cancelado": [],
    }
    member_totals: dict[str, Decimal] = {}
    for payment in payments:
        if payment.get("member_id"):
            member_key = f"member:{payment['member_id']}"
        else:
            source_name = (payment.get("source_name") or payment.get("full_name") or "").strip().lower()
            member_key = f"source:{source_name}"
        member_totals[member_key] = member_totals.get(member_key, Decimal("0")) + (
            payment.get("net_amount") or Decimal("0")
        )

    for payment in payments:
        if payment.get("member_id"):
            member_key = f"member:{payment['member_id']}"
        else:
            source_name = (payment.get("source_name") or payment.get("full_name") or "").strip().lower()
            member_key = f"source:{source_name}"
        payment["member_total_amount"] = member_totals.get(member_key, Decimal("0"))
        payment_groups.setdefault(payment.get("status") or "pendente", []).append(payment)
    rank_distribution = fetch_all(
        f"""
        select coalesce(m.rank, 'Sem posto') as rank_name, count(*) as total, coalesce(sum(fp.net_amount), 0) as amount
        from financial_payments fp
        left join members m on m.id = fp.member_id
        {where_sql}
        group by coalesce(m.rank, 'Sem posto')
        order by amount desc
        """,
        tuple(params),
    )
    reference_month_stats = fetch_all(
        """
        select
            date_format(reference_month, '%Y-%m') as month_key,
            date_format(reference_month, '%m/%Y') as month_label,
            count(*) as total
        from financial_payments
        where reference_month between %s and %s
        group by date_format(reference_month, '%Y-%m'), date_format(reference_month, '%m/%Y')
        order by month_key desc
        """,
        (FINANCIAL_REFERENCE_MIN, FINANCIAL_REFERENCE_MAX),
    )
    totals_by_month = {row["month_key"]: row["total"] for row in reference_month_stats}
    reference_months = [
        {
            "month_key": month_key,
            "month_label": datetime.strptime(f"{month_key}-01", "%Y-%m-%d").strftime("%m/%Y"),
            "total": totals_by_month.get(month_key, 0),
        }
        for month_key in allowed_reference_month_keys()
    ]
    import_history = fetch_all(
        """
        select
            fib.id,
            fib.reference_month,
            fib.input_mode,
            fib.source_name,
            fib.total_entries,
            fib.created_members,
            fib.created_payments,
            fib.updated_payments,
            fib.created_at,
            su.display_name as actor_name
        from financial_import_batches fib
        left join system_users su on su.id = fib.actor_system_user_id
        order by fib.created_at desc
        limit 30
        """
    )
    members_for_manual = fetch_all(
        """
        select id, full_name, `rank`, unit
        from members
        where status = 'ativo'
        order by full_name asc
        """
    )

    return render_template(
        "financeiro.html",
        metrics=metrics,
        payment_overview=payment_overview,
        filtered_metrics=filtered_metrics,
        pending_payments=payment_groups["pendente"],
        paid_payments=payment_groups["pago"],
        cancelled_payments=payment_groups["cancelado"],
        rank_distribution=rank_distribution,
        query=query,
        category=category,
        status=status,
        selected_sector=selected_sector,
        selected_reference_month=selected_reference_month,
        reference_months=reference_months,
        import_reference_month_default=selected_reference_month or reference_month_key(FINANCIAL_REFERENCE_MIN),
        reference_month_min=reference_month_key(FINANCIAL_REFERENCE_MIN),
        reference_month_max=reference_month_key(FINANCIAL_REFERENCE_MAX),
        import_history=import_history,
        sectors=sectors,
        saved=request.args.get("saved"),
        deleted=request.args.get("deleted"),
        error=request.args.get("error"),
        unmatched_count=int(request.args.get("unmatched_count") or 0),
        members_for_manual=members_for_manual,
    )


@app.post("/financeiro/processar")
def processar_financeiro_mes():
    denied = require_write_access()
    if denied:
        return denied

    ensure_sector_schema()
    ensure_financial_schema()
    ensure_financial_import_schema()

    try:
        reference_month = parse_reference_month_value(request.form.get("reference_month"))
    except ValueError:
        return redirect(url_for("financeiro", error="import_reference_month"))

    existing_batch = fetch_one(
        "select count(*) as total from financial_import_batches where reference_month = %s",
        (reference_month,),
    ) or {}
    existing_month_payments = fetch_one(
        "select count(*) as total from financial_payments where reference_month = %s",
        (reference_month,),
    ) or {}
    if int(existing_batch.get("total") or 0) > 0 or int(existing_month_payments.get("total") or 0) > 0:
        return redirect(
            url_for(
                "financeiro",
                reference_month=reference_month_key(reference_month),
                error="import_month_already_processed",
            )
        )

    source_text = (request.form.get("source_text") or "").strip()
    source_files = [f for f in request.files.getlist("source_file") if f and f.filename]
    input_mode = ""
    source_name = ""
    raw_content = ""
    entries = []

    if source_text:
        input_mode = "texto"
        source_name = "texto-colado"
        raw_content = source_text
        try:
            entries = parse_financial_entries(raw_content)
        except Exception:
            return redirect(url_for("financeiro", error="import_parse"))
    elif source_files:
        parsed_file_names = []
        raw_parts = []
        input_mode = "arquivo"
        for source_file in source_files:
            current_name = os.path.basename(source_file.filename)
            file_bytes = source_file.read()
            try:
                decoded_content = file_bytes.decode("utf-8-sig")
            except UnicodeDecodeError:
                decoded_content = file_bytes.decode("latin-1")
            try:
                current_entries = parse_financial_entries(decoded_content)
            except Exception:
                return redirect(url_for("financeiro", error="import_parse"))
            if current_entries:
                entries.extend(current_entries)
            parsed_file_names.append(current_name)
            raw_parts.append(decoded_content)
        source_name = ", ".join(parsed_file_names)
        if len(parsed_file_names) > 1:
            input_mode = "arquivos"
        raw_content = "\n\n".join(raw_parts)
    else:
        return redirect(url_for("financeiro", error="import_missing_source"))

    if not entries:
        return redirect(url_for("financeiro", error="import_parse"))

    created_members = 0
    created_payments = 0
    updated_payments = 0
    unmatched_entries = 0

    with db() as connection:
        cursor = connection.cursor()
        for entry in entries:
            member_id = find_member_for_import(cursor, entry)
            if not member_id:
                unmatched_entries += 1
                continue

        if unmatched_entries > 0:
            return redirect(
                url_for(
                    "financeiro",
                    reference_month=reference_month_key(reference_month),
                    error="import_unmatched",
                    unmatched_count=unmatched_entries,
                )
            )

        for entry in entries:
            member_id = find_member_for_import(cursor, entry)
            if not member_id:
                continue

            payment_department = entry.get("department")
            if not payment_department:
                payment_department = infer_department_for_member(cursor, member_id)

            source_key = source_key_for_entry(entry, reference_month)
            cursor.execute("select id from financial_payments where source_key = %s", (source_key,))
            existing_payment = cursor.fetchone()

            payment_values = (
                member_id,
                reference_month,
                "beneficio" if entry["category"] == "Administrativo" else "gratificacao",
                entry["category"],
                payment_department,
                entry["name"],
                entry.get("total_minutes"),
                entry.get("extra_minutes"),
                entry.get("function_count"),
                entry.get("functions_label"),
                entry["amount"],
                Decimal("0"),
                entry["amount"],
                "pendente",
                None,
                f"Importado no processamento mensal ({reference_month.strftime('%m/%Y')}).",
            )

            if existing_payment:
                cursor.execute(
                    """
                    update financial_payments
                    set member_id = %s,
                        reference_month = %s,
                        payment_type = %s,
                        source_category = %s,
                        department = %s,
                        source_name = %s,
                        total_minutes = %s,
                        extra_minutes = %s,
                        function_count = %s,
                        functions_label = %s,
                        gross_amount = %s,
                        deductions = %s,
                        net_amount = %s,
                        status = %s,
                        paid_at = %s,
                        notes = %s,
                        updated_at = now()
                    where source_key = %s
                    """,
                    payment_values + (source_key,),
                )
                updated_payments += 1
            else:
                cursor.execute(
                    """
                    insert into financial_payments
                        (source_key, member_id, reference_month, payment_type, source_category,
                         department, source_name, total_minutes, extra_minutes, function_count, functions_label,
                         gross_amount, deductions, net_amount, status, paid_at, notes, created_at, updated_at)
                    values
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                    """,
                    (source_key,) + payment_values,
                )
                created_payments += 1

        cursor.execute(
            """
            insert ignore into sectors (name, status, created_at, updated_at)
            select distinct unit, 'ativo', now(), now()
            from members
            where unit is not null and unit <> ''
            """
        )
        cursor.execute(
            """
            insert ignore into member_sectors (member_id, sector_id, created_at)
            select m.id, s.id, now()
            from members m
            inner join sectors s on s.name = m.unit
            where m.unit is not null and m.unit <> ''
            """
        )
        cursor.execute(
            """
            insert into financial_import_batches
                (reference_month, input_mode, source_name, source_preview, total_entries,
                 created_members, created_payments, updated_payments, actor_system_user_id, created_at)
            values
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            """,
            (
                reference_month,
                input_mode,
                source_name,
                raw_content[:3000],
                len(entries),
                created_members,
                created_payments,
                updated_payments,
                current_auth_user().get("system_user_id"),
            ),
        )
        import_batch_id = cursor.lastrowid
        connection.commit()

    if created_payments == 0 and updated_payments == 0:
        return redirect(url_for("financeiro", error="import_parse"))

    log_audit_event(
        "financeiro",
        "financeiro_processado_mensalmente",
        target_type="importacao_financeira",
        target_id=import_batch_id,
        target_label=f"{reference_month.strftime('%m/%Y')} - {source_name}",
        details={
            "reference_month": reference_month.isoformat(),
            "input_mode": input_mode,
            "source_name": source_name,
            "total_entries": len(entries),
            "created_members": created_members,
            "unmatched_entries": unmatched_entries,
            "created_payments": created_payments,
            "updated_payments": updated_payments,
        },
    )

    return redirect(
        url_for(
            "financeiro",
            reference_month=reference_month_key(reference_month),
            saved="imported",
        )
    )


@app.post("/financeiro/excluir-processamento")
def excluir_processamento_mes():
    denied = require_write_access()
    if denied:
        return denied

    ensure_financial_schema()
    ensure_financial_import_schema()

    try:
        reference_month = parse_reference_month_value(request.form.get("reference_month"))
    except ValueError:
        return redirect(url_for("financeiro", error="import_reference_month"))

    payments_row = fetch_one(
        "select count(*) as total from financial_payments where reference_month = %s",
        (reference_month,),
    ) or {}
    deleted_payments = int(payments_row.get("total") or 0)

    batches_row = fetch_one(
        "select count(*) as total from financial_import_batches where reference_month = %s",
        (reference_month,),
    ) or {}
    deleted_batches = int(batches_row.get("total") or 0)

    if deleted_payments == 0 and deleted_batches == 0:
        return redirect(
            url_for(
                "financeiro",
                reference_month=reference_month_key(reference_month),
                error="delete_month_empty",
            )
        )

    with db() as connection:
        cursor = connection.cursor()
        cursor.execute("delete from financial_payments where reference_month = %s", (reference_month,))
        cursor.execute("delete from financial_import_batches where reference_month = %s", (reference_month,))
        connection.commit()

    log_audit_event(
        "financeiro",
        "processamento_mensal_excluido",
        target_type="importacao_financeira",
        target_label=reference_month.strftime("%m/%Y"),
        details={
            "reference_month": reference_month.isoformat(),
            "deleted_payments": deleted_payments,
            "deleted_batches": deleted_batches,
        },
    )

    return redirect(
        url_for(
            "financeiro",
            reference_month=reference_month_key(reference_month),
            deleted="month",
        )
    )


@app.post("/financeiro/adicionar-avulso")
def adicionar_pagamento_avulso():
    denied = require_write_access()
    if denied:
        return denied

    ensure_financial_schema()

    try:
        reference_month = parse_reference_month_value(request.form.get("reference_month"))
        member_id = form_int("member_id", required=True)
        source_category = form_text("source_category", required=True)
        gross_amount = form_money("gross_amount", required=True)
        deductions = form_money("deductions") or Decimal("0")
        status = form_text("status", required=True)
        function_count = form_int("function_count")
        total_minutes = form_minutes("total_minutes")
        extra_minutes = form_minutes("extra_minutes")
    except ValueError:
        return redirect(url_for("financeiro", error="manual_add"))

    if source_category not in {"Horas", "Administrativo"} or status not in {"pendente", "pago", "cancelado"}:
        return redirect(url_for("financeiro", error="manual_add"))

    member = fetch_one(
        """
        select id, full_name, `rank`, unit
        from members
        where id = %s
        """,
        (member_id,),
    )
    if not member:
        return redirect(url_for("financeiro", error="manual_add"))

    source_name = (form_text("source_name") or member.get("full_name") or "").strip()
    department = (form_text("department") or member.get("unit") or "").strip() or None
    functions_label = form_text("functions_label")
    notes = form_text("notes")
    if not notes:
        notes = f"Lancamento avulso manual ({reference_month.strftime('%m/%Y')})."

    entry = {
        "category": source_category,
        "name": source_name,
        "amount": gross_amount,
        "department": department if source_category == "Administrativo" else None,
        "total_minutes": total_minutes if source_category == "Horas" else None,
        "extra_minutes": extra_minutes if source_category == "Horas" else None,
        "function_count": function_count if source_category == "Administrativo" else None,
        "functions_label": functions_label if source_category == "Administrativo" else None,
    }
    source_key = source_key_for_entry(entry, reference_month)
    existing = fetch_one("select id from financial_payments where source_key = %s", (source_key,))
    if existing:
        return redirect(
            url_for(
                "financeiro",
                reference_month=reference_month_key(reference_month),
                error="manual_duplicate",
            )
        )

    paid_at_value = date.today() if status == "pago" else None
    execute(
        """
        insert into financial_payments
            (source_key, member_id, reference_month, payment_type, source_category,
             department, source_name, total_minutes, extra_minutes, function_count, functions_label,
             gross_amount, deductions, net_amount, status, paid_at, notes, created_at, updated_at)
        values
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        """,
        (
            source_key,
            member_id,
            reference_month,
            "beneficio" if source_category == "Administrativo" else "gratificacao",
            source_category,
            department if source_category == "Administrativo" else None,
            source_name,
            total_minutes if source_category == "Horas" else None,
            extra_minutes if source_category == "Horas" else None,
            function_count if source_category == "Administrativo" else None,
            functions_label if source_category == "Administrativo" else None,
            gross_amount,
            deductions,
            gross_amount - deductions,
            status,
            paid_at_value,
            notes,
        ),
    )
    log_audit_event(
        "financeiro",
        "pagamento_avulso_adicionado",
        target_type="pagamento",
        target_label=source_name,
        details={
            "member_id": member_id,
            "reference_month": reference_month.isoformat(),
            "source_category": source_category,
            "gross_amount": str(gross_amount),
            "deductions": str(deductions),
            "status": status,
            "functions_label": functions_label,
        },
    )
    return redirect(
        url_for(
            "financeiro",
            reference_month=reference_month_key(reference_month),
            q=source_name,
            saved="manual_added",
        )
    )


@app.get("/relatorio-financeiro")
def relatorio_financeiro():
    sectors = sector_options()
    ensure_financial_schema()
    sync_financial_members()
    selected_sector = request.args.get("sector", "").strip()
    selected_status = request.args.get("status", "").strip().lower()
    allowed_statuses = {"pendente", "pago", "cancelado"}
    if selected_status not in allowed_statuses:
        selected_status = ""

    outer_params: list = []
    where = []

    if selected_sector:
        where.append("s.name = %s")
        outer_params.append(selected_sector)

    where.append("(admin_payments.administrative_amount is not null or hour_payments.hourly_amount is not null)")
    where_sql = "where " + " and ".join(where)
    payment_status_sql = " and status = %s" if selected_status else ""
    row_params: list = []
    if selected_status:
        row_params.extend([selected_status, selected_status])
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
            where source_category = 'Administrativo'{payment_status_sql}
            group by member_id, department
        ) admin_payments on admin_payments.member_id = m.id and admin_payments.department = s.name
        left join (
            select member_id, sum(net_amount) as hourly_amount, sum(extra_minutes) as extra_minutes
            from financial_payments
            where source_category = 'Horas'{payment_status_sql}
            group by member_id
        ) hour_payments on hour_payments.member_id = m.id
        {where_sql}
        order by s.name asc, total_amount desc, m.full_name asc
        """,
        tuple(row_params + outer_params),
    )

    sector_scope_where = ""
    sector_scope_params: list = []
    if selected_sector:
        sector_scope_where = """
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
        """
        sector_scope_params = [selected_sector, selected_sector]

    status_overview = fetch_one(
        f"""
        select
            count(*) as total_count,
            coalesce(sum(net_amount), 0) as total_amount,
            coalesce(sum(case when fp.status = 'pago' then 1 else 0 end), 0) as paid_count,
            coalesce(sum(case when fp.status = 'pago' then net_amount else 0 end), 0) as paid_amount,
            coalesce(sum(case when fp.status = 'pendente' then 1 else 0 end), 0) as pending_count,
            coalesce(sum(case when fp.status = 'pendente' then net_amount else 0 end), 0) as pending_amount,
            coalesce(sum(case when fp.status = 'cancelado' then 1 else 0 end), 0) as cancelled_count,
            coalesce(sum(case when fp.status = 'cancelado' then net_amount else 0 end), 0) as cancelled_amount
        from financial_payments fp
        left join members m on m.id = fp.member_id
        {sector_scope_where}
        """,
        tuple(sector_scope_params),
    )

    if selected_sector:
        totals_params: list = [selected_sector, selected_sector]
        status_filter_sql = ""
        if selected_status:
            status_filter_sql = "and fp.status = %s"
            totals_params.append(selected_status)
        report_totals = fetch_one(
            f"""
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
            {status_filter_sql}
            """,
            tuple(totals_params),
        )
    else:
        totals_where_sql = "where status = %s" if selected_status else ""
        totals_params = (selected_status,) if selected_status else ()
        report_totals = fetch_one(
            f"""
            select
                count(distinct member_id) as members_count,
                coalesce(sum(case when source_category = 'Administrativo' then net_amount else 0 end), 0) as administrative_amount,
                coalesce(sum(case when source_category = 'Horas' then net_amount else 0 end), 0) as hourly_amount,
                coalesce(sum(case when source_category = 'Horas' then extra_minutes else 0 end), 0) as extra_minutes
            from financial_payments
            {totals_where_sql}
            """,
            totals_params,
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
        selected_status=selected_status,
        report=list(report.values()),
        totals=totals,
        status_overview=status_overview,
    )


@app.get("/demonstrativo-financeiro")
def demonstrativo_financeiro():
    ensure_sector_schema()
    ensure_financial_schema()
    sync_financial_members()
    sectors = sector_options()
    selected_sector = request.args.get("sector", "").strip()
    selected_status = request.args.get("status", "").strip().lower()
    selected_category = request.args.get("category", "").strip()
    selected_reference_month = normalize_reference_month_arg(request.args.get("reference_month"))
    if not selected_reference_month:
        selected_reference_month = reference_month_key(FINANCIAL_REFERENCE_MIN)
    allowed_statuses = {"pendente", "pago", "cancelado"}
    if selected_status not in allowed_statuses:
        selected_status = ""

    params: list = []
    where = []

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

    if selected_status:
        where.append("fp.status = %s")
        params.append(selected_status)

    if selected_category:
        where.append("fp.source_category = %s")
        params.append(selected_category)
    where.append("date_format(fp.reference_month, '%Y-%m') = %s")
    params.append(selected_reference_month)

    where_sql = "where " + " and ".join(where) if where else ""
    rows = fetch_all(
        f"""
        select
            fp.id,
            coalesce(fp.department, member_sector_map.sector_names, m.unit, 'Sem setor') as sector_name,
            coalesce(fp.source_name, m.full_name, 'Sem origem') as source_name,
            m.full_name,
            m.registration_number,
            m.`rank`,
            m.role,
            fp.source_category,
            fp.function_count,
            fp.functions_label,
            fp.total_minutes,
            fp.extra_minutes,
            fp.gross_amount,
            fp.deductions,
            fp.net_amount,
            fp.status,
            fp.notes,
            fp.paid_at
        from financial_payments fp
        left join members m on m.id = fp.member_id
        left join (
            select ms.member_id, group_concat(s.name order by s.name separator ' + ') as sector_names
            from member_sectors ms
            inner join sectors s on s.id = ms.sector_id
            group by ms.member_id
        ) member_sector_map on member_sector_map.member_id = m.id
        {where_sql}
        order by
            sector_name asc,
            case fp.status
                when 'pendente' then 0
                when 'pago' then 1
                when 'cancelado' then 2
                else 3
            end,
            fp.source_category asc,
            fp.net_amount desc,
            source_name asc
        """,
        tuple(params),
    )
    overview = fetch_one(
        f"""
        select
            count(*) as total_count,
            coalesce(sum(fp.gross_amount), 0) as gross_amount,
            coalesce(sum(fp.deductions), 0) as deductions_amount,
            coalesce(sum(fp.net_amount), 0) as net_amount,
            coalesce(sum(case when fp.status = 'pago' then 1 else 0 end), 0) as paid_count,
            coalesce(sum(case when fp.status = 'pago' then fp.net_amount else 0 end), 0) as paid_amount,
            coalesce(sum(case when fp.status = 'pendente' then 1 else 0 end), 0) as pending_count,
            coalesce(sum(case when fp.status = 'pendente' then fp.net_amount else 0 end), 0) as pending_amount,
            coalesce(sum(case when fp.status = 'cancelado' then 1 else 0 end), 0) as cancelled_count,
            coalesce(sum(case when fp.status = 'cancelado' then fp.net_amount else 0 end), 0) as cancelled_amount
        from financial_payments fp
        left join members m on m.id = fp.member_id
        {where_sql}
        """,
        tuple(params),
    )

    report: dict[str, dict] = {}
    for row in rows:
        sector_name = row.get("sector_name") or "Sem setor"
        sector = report.setdefault(
            sector_name,
            {
                "name": sector_name,
                "entries": [],
                "total_count": 0,
                "gross_amount": Decimal("0"),
                "deductions_amount": Decimal("0"),
                "net_amount": Decimal("0"),
                "paid_count": 0,
                "pending_count": 0,
                "cancelled_count": 0,
            },
        )
        sector["entries"].append(row)
        sector["total_count"] += 1
        sector["gross_amount"] += row["gross_amount"] or Decimal("0")
        sector["deductions_amount"] += row["deductions"] or Decimal("0")
        sector["net_amount"] += row["net_amount"] or Decimal("0")
        if row.get("status") == "pago":
            sector["paid_count"] += 1
        elif row.get("status") == "pendente":
            sector["pending_count"] += 1
        elif row.get("status") == "cancelado":
            sector["cancelled_count"] += 1

    return render_template(
        "demonstrativo_financeiro.html",
        sectors=sectors,
        selected_sector=selected_sector,
        selected_status=selected_status,
        selected_category=selected_category,
        selected_reference_month=selected_reference_month,
        report=list(report.values()),
        overview=overview,
        generated_at=datetime.now(),
    )


@app.post("/financeiro/<int:payment_id>")
def atualizar_pagamento(payment_id: int):
    denied = require_write_access()
    if denied:
        return denied

    ensure_financial_schema()
    query = request.form.get("q", "").strip()
    category_filter = request.form.get("category_filter", "").strip()
    status_filter = request.form.get("status_filter", "").strip()
    sector_filter = request.form.get("sector_filter", "").strip()
    reference_month_filter = request.form.get("reference_month_filter", "").strip()
    is_autosave = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    previous_payment = fetch_one(
        """
        select id, source_name, source_category, department, gross_amount, deductions, net_amount, status,
               total_minutes, extra_minutes, function_count, functions_label, notes
        from financial_payments
        where id = %s
        """,
        (payment_id,),
    )

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
        return redirect(
            url_for(
                "financeiro",
                q=query,
                category=category_filter,
                status=status_filter,
                sector=sector_filter,
                reference_month=reference_month_filter,
                error="payment",
            )
        )

    execute(
        """
        update financial_payments
        set gross_amount = %s,
            deductions = %s,
            net_amount = %s,
            status = %s,
            paid_at = case
                when %s = 'pago' then coalesce(paid_at, curdate())
                else null
            end,
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
    next_values = {
        "source_category": source_category,
        "department": (department or sector_filter) if source_category == "Administrativo" else None,
        "gross_amount": gross_amount,
        "deductions": deductions,
        "net_amount": gross_amount - deductions,
        "status": status,
        "total_minutes": total_minutes,
        "extra_minutes": extra_minutes,
        "function_count": function_count,
        "functions_label": functions_label,
        "notes": notes,
    }
    payment_changes = changed_fields(previous_payment, next_values)
    if payment_changes:
        log_audit_event(
            "financeiro",
            "pagamento_atualizado",
            target_type="pagamento",
            target_id=payment_id,
            target_label=previous_payment.get("source_name"),
            details={
                "autosave": is_autosave,
                "changes": payment_changes,
            },
        )

    if is_autosave:
        return {"ok": True, "net_amount": money(gross_amount - deductions)}

    return redirect(
        url_for(
            "financeiro",
            q=query,
            category=category_filter,
            status=status_filter,
            sector=sector_filter,
            reference_month=reference_month_filter,
            saved="1",
        )
    )


@app.post("/financeiro/<int:payment_id>/excluir")
def excluir_pagamento(payment_id: int):
    denied = require_write_access()
    if denied:
        return denied

    ensure_financial_schema()
    query = request.form.get("q", "").strip()
    category_filter = request.form.get("category_filter", "").strip()
    status_filter = request.form.get("status_filter", "").strip()
    sector_filter = request.form.get("sector_filter", "").strip()
    reference_month_filter = request.form.get("reference_month_filter", "").strip()
    payment = fetch_one(
        """
        select id, source_name, source_category, department, net_amount, status
        from financial_payments
        where id = %s
        """,
        (payment_id,),
    )

    execute("delete from financial_payments where id = %s", (payment_id,))
    log_audit_event(
        "financeiro",
        "pagamento_excluido",
        target_type="pagamento",
        target_id=payment_id,
        target_label=payment.get("source_name"),
        details=payment,
    )
    return redirect(
        url_for(
            "financeiro",
            q=query,
            category=category_filter,
            status=status_filter,
            sector=sector_filter,
            reference_month=reference_month_filter,
            deleted="1",
        )
    )


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
    denied = require_write_access()
    if denied:
        return denied

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

    created_sector = fetch_one("select id from sectors where name = %s", (name,))
    log_audit_event(
        "cadastro",
        "setor_criado",
        target_type="setor",
        target_id=created_sector.get("id"),
        target_label=name,
        details={"description": description, "status": status},
    )
    return redirect(url_for("setores", saved="created"))


@app.post("/setores/<int:sector_id>")
def atualizar_setor(sector_id: int):
    denied = require_write_access()
    if denied:
        return denied

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
    previous_sector = fetch_one(
        """
        select id, name, description, status
        from sectors
        where id = %s
        """,
        (sector_id,),
    )
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

    log_audit_event(
        "cadastro",
        "setor_atualizado",
        target_type="setor",
        target_id=sector_id,
        target_label=name,
        details={
            "changes": changed_fields(
                previous_sector,
                {
                    "name": name,
                    "description": description,
                    "status": status,
                },
            ),
            "old_name": old_name,
        },
    )
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


@app.post("/membros")
def criar_membro():
    denied = require_write_access()
    if denied:
        return denied

    ensure_sector_schema()
    query = request.form.get("q", "").strip()
    selected_sector = request.form.get("sector_filter", "").strip()

    try:
        full_name = form_text("full_name", required=True)
        registration_number = form_text("registration_number", required=True)
        rank = form_text("rank", required=True)
        unit = form_text("unit", required=True)
        role = form_text("role", required=True)
        status = form_text("status", required=True)
    except ValueError:
        return redirect(url_for("membros", q=query, sector=selected_sector, error="missing"))

    units: list[str] = []
    for sector_name in [unit, *request.form.getlist("units")]:
        normalized_sector = sector_name.strip()
        if normalized_sector and normalized_sector not in units:
            units.append(normalized_sector)

    try:
        with db() as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                insert into members
                    (full_name, registration_number, `rank`, unit, role, status, created_at, updated_at)
                values
                    (%s, %s, %s, %s, %s, %s, now(), now())
                """,
                (
                    full_name,
                    registration_number,
                    rank,
                    unit,
                    role,
                    status,
                ),
            )
            member_id = cursor.lastrowid

            for sector_name in units:
                cursor.execute(
                    """
                    insert ignore into sectors (name, status, created_at, updated_at)
                    values (%s, 'ativo', now(), now())
                    """,
                    (sector_name,),
                )
                cursor.execute(
                    """
                    insert ignore into member_sectors (member_id, sector_id, created_at)
                    select %s, id, now()
                    from sectors
                    where name = %s
                    """,
                    (member_id, sector_name),
                )

            connection.commit()
    except IntegrityError:
        return redirect(url_for("membros", q=query, sector=selected_sector, error="duplicate"))

    log_audit_event(
        "membros",
        "membro_criado",
        target_type="membro",
        target_id=member_id,
        target_label=full_name,
        details={
            "registration_number": registration_number,
            "rank": rank,
            "role": role,
            "status": status,
            "sectors": units,
        },
    )
    return redirect(url_for("membros", sector=unit, saved="created"))


@app.post("/membros/sem-setor-adm")
def marcar_sem_setor_adm():
    denied = require_write_access()
    if denied:
        return denied

    ensure_sector_schema()
    query = request.form.get("q", "").strip()
    selected_sector = request.form.get("sector_filter", "").strip()
    affected_rows = fetch_all(
        """
        select m.full_name
        from members m
        where not exists (
            select 1
            from financial_payments fp
            where fp.member_id = m.id
                and fp.source_category = 'Administrativo'
        )
        order by m.full_name asc
        """
    )

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
    log_audit_event(
        "membros",
        "membros_marcados_sem_setor_adm",
        details={
            "query": query,
            "sector_filter": selected_sector,
            "target_sector": NO_ADMIN_SECTOR,
            "affected_count": len(affected_rows),
            "affected_members": [row["full_name"] for row in affected_rows[:15] if row.get("full_name")],
        },
    )

    return redirect(url_for("membros", q=query, sector=selected_sector, saved="no_admin"))


@app.post("/membros/unificar-duplicados")
def unificar_membros_duplicados():
    denied = require_write_access()
    if denied:
        return denied

    ensure_sector_schema()
    sync_financial_members()
    query = request.form.get("q", "").strip()
    selected_sector = request.form.get("sector_filter", "").strip()
    merge_result = merge_duplicate_members_by_identity()
    merged_count = merge_result.get("merged_count", 0)
    log_audit_event(
        "membros",
        "duplicados_unificados",
        details={
            "merged_count": merged_count,
            "merge_items": merge_result.get("items", []),
            "query": query,
            "sector_filter": selected_sector,
        },
    )
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
    denied = require_write_access()
    if denied:
        return denied

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

    merge_details = merge_member_records(canonical_id, duplicate_id)
    if not merge_details:
        return redirect(url_for("membros", q=query, sector=selected_sector, error="merge_missing"))

    log_audit_event(
        "membros",
        "membros_acoplados",
        target_type="membro",
        target_id=canonical_id,
        details={
            **merge_details,
            "query": query,
            "sector_filter": selected_sector,
        },
    )
    return redirect(url_for("membros", q=query, sector=selected_sector, saved="manual_merged"))


@app.post("/membros/importar-registros")
def importar_registros_membros():
    denied = require_write_access()
    if denied:
        return denied

    query = request.form.get("q", "").strip()
    selected_sector = request.form.get("sector_filter", "").strip()
    backup_json = request.form.get("backup_json", "").strip()
    if not backup_json:
        return redirect(url_for("membros", q=query, sector=selected_sector, error="import_missing"))

    try:
        payload = json.loads(backup_json)
    except json.JSONDecodeError:
        return redirect(url_for("membros", q=query, sector=selected_sector, error="import_invalid"))

    if not isinstance(payload, list):
        return redirect(url_for("membros", q=query, sector=selected_sector, error="import_invalid"))

    incoming_by_rg: dict[str, dict] = {}
    skipped_invalid = 0
    for item in payload:
        if not isinstance(item, dict):
            skipped_invalid += 1
            continue
        rg = str(item.get("rg") or "").strip()
        nome = str(item.get("nome") or "").strip()
        if not rg or not nome:
            skipped_invalid += 1
            continue
        incoming_by_rg[rg] = {
            "full_name": nome,
            "rank": normalize_backup_rank(item.get("patente")),
        }

    if not incoming_by_rg:
        return redirect(url_for("membros", q=query, sector=selected_sector, error="import_invalid"))

    placeholders = ",".join(["%s"] * len(incoming_by_rg))
    rows = fetch_all(
        f"""
        select id, registration_number, full_name, `rank`
        from members
        where registration_number in ({placeholders})
        """,
        tuple(incoming_by_rg.keys()),
    )
    existing_by_rg = {str(row.get("registration_number") or "").strip(): row for row in rows}

    updated_count = 0
    unchanged_count = 0
    not_found_count = 0
    statements: list[tuple[str, tuple]] = []

    for rg, incoming in incoming_by_rg.items():
        existing = existing_by_rg.get(rg)
        if not existing:
            not_found_count += 1
            continue

        old_name = existing.get("full_name") or ""
        old_rank = existing.get("rank") or ""
        new_name = incoming["full_name"]
        new_rank = incoming["rank"]
        if old_name == new_name and old_rank == new_rank:
            unchanged_count += 1
            continue

        member_id = int(existing["id"])
        statements.append(
            (
                """
                update members
                set full_name = %s,
                    `rank` = %s,
                    updated_at = now()
                where id = %s
                """,
                (new_name, new_rank, member_id),
            )
        )
        if old_name != new_name:
            statements.append(
                (
                    """
                    update financial_payments
                    set source_name = %s,
                        updated_at = now()
                    where member_id = %s
                    """,
                    (new_name, member_id),
                )
            )
        updated_count += 1

    if statements:
        execute_transaction(statements)

    log_audit_event(
        "membros",
        "importacao_registros_nome_posto",
        details={
            "updated_count": updated_count,
            "unchanged_count": unchanged_count,
            "not_found_count": not_found_count,
            "skipped_invalid": skipped_invalid,
            "records_received": len(payload),
            "records_valid": len(incoming_by_rg),
        },
    )

    return redirect(
        url_for(
            "membros",
            q=query,
            sector=selected_sector,
            saved="imported",
            updated=updated_count,
            unchanged=unchanged_count,
            not_found=not_found_count,
            skipped=skipped_invalid,
        )
    )


@app.post("/membros/<int:member_id>")
def atualizar_membro(member_id: int):
    denied = require_write_access()
    if denied:
        return denied

    ensure_sector_schema()
    query = request.form.get("q", "").strip()
    selected_sector = request.form.get("sector_filter", "").strip()
    is_autosave = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    previous_member = fetch_one(
        """
        select id, full_name, registration_number, `rank`, unit, role, status
        from members
        where id = %s
        """,
        (member_id,),
    )
    previous_member["sectors"] = member_sector_names(member_id)

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

    member_changes = changed_fields(
        previous_member,
        {
            "full_name": full_name,
            "registration_number": registration_number,
            "rank": rank,
            "unit": unit,
            "role": role,
            "status": status,
            "sectors": sorted(set(units)),
        },
    )
    if member_changes or previous_member.get("unit") != unit or selected_sector != unit:
        log_audit_event(
            "membros",
            "membro_atualizado",
            target_type="membro",
            target_id=member_id,
            target_label=full_name,
            details={
                "autosave": is_autosave,
                "changes": member_changes,
                "sectors": units,
            },
        )

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
        log_audit_event(
            "inscricao",
            "inscricao_criada",
            target_type="inscricao",
            target_label=form.get("full_name"),
            details={
                "registration_number": form.get("registration_number"),
                "rank": form.get("rank"),
                "unit": form.get("unit"),
                "email": form.get("email"),
            },
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
