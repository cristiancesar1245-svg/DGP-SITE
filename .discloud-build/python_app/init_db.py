from __future__ import annotations

import argparse
import hashlib
import os
import re
import unicodedata
import zlib
from dataclasses import dataclass

import mysql.connector


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DATA_FILE = os.path.join(ROOT_DIR, "data", "message.txt")

DB_HOST = os.getenv("DGP_DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DGP_DB_PORT", "3308"))
DB_DATABASE = os.getenv("DGP_DB_DATABASE", "dgp")
DB_USERNAME = os.getenv("DGP_DB_USERNAME", "root")
DB_PASSWORD = os.getenv("DGP_DB_PASSWORD", "12457803")
NO_ADMIN_SECTOR = "SEM SETOR ADM"


@dataclass(frozen=True)
class FinancialEntry:
    category: str
    name: str
    rank: str
    amount: int
    total_minutes: int | None = None
    extra_minutes: int | None = None
    function_count: int | None = None
    functions_label: str | None = None
    department: str | None = None

    @property
    def source_key(self) -> str:
        raw = "|".join(
            [
                self.category,
                self.name,
                str(self.amount),
                str(self.total_minutes or ""),
                str(self.extra_minutes or ""),
                self.functions_label or "",
            ]
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def connect(database: str | None = DB_DATABASE):
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=database,
        user=DB_USERNAME,
        password=DB_PASSWORD,
        charset="utf8mb4",
    )


def create_database() -> None:
    with connect(database=None) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"create database if not exists `{DB_DATABASE}` character set utf8mb4 collate utf8mb4_unicode_ci"
        )
        conn.commit()


def create_schema(reset: bool = False) -> None:
    with connect() as conn:
        cursor = conn.cursor()

        if reset:
            cursor.execute("set foreign_key_checks = 0")
            cursor.execute("drop table if exists system_users")
            cursor.execute("drop table if exists financial_payments")
            cursor.execute("drop table if exists department_applications")
            cursor.execute("drop table if exists member_sectors")
            cursor.execute("drop table if exists members")
            cursor.execute("drop table if exists sectors")
            cursor.execute("set foreign_key_checks = 1")

        cursor.execute(
            """
            create table if not exists sectors (
                id bigint unsigned not null auto_increment primary key,
                name varchar(120) not null unique,
                description varchar(255) null,
                status varchar(30) not null default 'ativo',
                created_at timestamp null,
                updated_at timestamp null
            ) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci
            """
        )
        cursor.execute(
            """
            insert ignore into sectors (name, description, status, created_at, updated_at)
            values (%s, 'Policiais sem setor administrativo definido.', 'ativo', now(), now())
            """,
            (NO_ADMIN_SECTOR,),
        )

        cursor.execute(
            """
            create table if not exists members (
                id bigint unsigned not null auto_increment primary key,
                full_name varchar(255) not null,
                registration_number varchar(50) not null unique,
                `rank` varchar(80) not null,
                unit varchar(120) not null,
                role varchar(120) not null,
                email varchar(255) null,
                phone varchar(30) null,
                admission_date date null,
                status varchar(30) not null default 'ativo',
                created_at timestamp null,
                updated_at timestamp null
            ) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci
            """
        )

        cursor.execute(
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
            """
        )

        cursor.execute(
            """
            create table if not exists financial_payments (
                id bigint unsigned not null auto_increment primary key,
                source_key varchar(64) null unique,
                member_id bigint unsigned null,
                reference_month date not null,
                payment_type varchar(40) not null default 'gratificacao',
                source_category varchar(40) null,
                department varchar(120) null,
                source_name varchar(255) null,
                total_minutes int unsigned null,
                extra_minutes int unsigned null,
                function_count tinyint unsigned null,
                functions_label text null,
                gross_amount decimal(12, 2) not null default 0,
                deductions decimal(12, 2) not null default 0,
                net_amount decimal(12, 2) not null default 0,
                status varchar(30) not null default 'pago',
                paid_at date null,
                notes text null,
                created_at timestamp null,
                updated_at timestamp null,
                constraint financial_payments_member_id_foreign
                    foreign key (member_id) references members(id)
                    on delete set null
            ) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci
            """
        )

        cursor.execute(
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

        cursor.execute(
            """
            create table if not exists department_applications (
                id bigint unsigned not null auto_increment primary key,
                full_name varchar(255) not null,
                registration_number varchar(50) not null,
                `rank` varchar(80) not null,
                unit varchar(120) not null,
                email varchar(255) null,
                phone varchar(30) null,
                motivation text not null,
                status varchar(30) not null default 'pendente',
                submitted_at timestamp null,
                review_notes text null,
                created_at timestamp null,
                updated_at timestamp null
            ) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_unicode_ci
            """
        )
        conn.commit()


def money_to_int(value: str) -> int:
    return int(value.replace(".", ""))


def minutes(hours: str, mins: str) -> int:
    return (int(hours) * 60) + int(mins)


def rank_from_name(name: str) -> str:
    lower = name.lower()
    ranks = [
        ("t.coronel", "T.Coronel"),
        ("coronel", "Coronel"),
        ("capit", "Capitao"),
        ("1.tenente", "1.Tenente"),
        ("2.tenente", "2.Tenente"),
        ("s.tenente", "S.Tenente"),
        ("aspirante", "Aspirante"),
        ("1.sargento", "1.Sargento"),
        ("2.sargento", "2.Sargento"),
        ("3.sargento", "3.Sargento"),
        ("cabo", "Cabo"),
        ("sd 1", "Sd 1a Cl"),
    ]

    for needle, rank in ranks:
        if needle in lower:
            return rank

    return "Outros"


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


def parse_file(path: str) -> list[FinancialEntry]:
    entries: list[FinancialEntry] = []
    section = "Horas"
    hourly_pattern = re.compile(
        r"^@(.+?)\s+-\s+(\d+):(\d+)(?:\s+\(extra\s+(\d+):(\d+)\))?\s+-\s+R\$\s+([\d.]+)$"
    )
    administrative_pattern = re.compile(
        r"^@(.+?)\s+-\s+(\d+)\s+fun\S*\s+\((.*?)\)\s+-\s+R\$\s+([\d.]+)$"
    )

    with open(path, "r", encoding="utf-8-sig") as file:
        for raw_line in file:
            line = raw_line.strip()
            if not line:
                continue

            if line.startswith("#"):
                section = "Administrativo" if "setores" in line.lower() else "Horas"
                continue

            if not line.startswith("@"):
                continue

            if section == "Administrativo":
                match = administrative_pattern.match(line)
                if not match:
                    continue
                name, function_count, functions_label, amount = match.groups()
                entries.append(
                    FinancialEntry(
                        category="Administrativo",
                        name=name.strip(),
                        rank=rank_from_name(name),
                        amount=money_to_int(amount),
                        function_count=int(function_count),
                        functions_label=" + ".join(part.strip() for part in functions_label.split("+")),
                    )
                )
                continue

            match = hourly_pattern.match(line)
            if not match:
                continue

            name, total_hour, total_min, extra_hour, extra_min, amount = match.groups()
            entries.append(
                FinancialEntry(
                    category="Horas",
                    name=name.strip(),
                    rank=rank_from_name(name),
                    amount=money_to_int(amount),
                    total_minutes=minutes(total_hour, total_min),
                    extra_minutes=minutes(extra_hour, extra_min) if extra_hour and extra_min else 0,
                )
            )

    return entries


def registration_for(name: str) -> str:
    return "IMP-" + f"{zlib.crc32(name.encode('utf-8')) & 0xffffffff:08X}"


def upsert_member(cursor, entry: FinancialEntry) -> tuple[int, bool]:
    registration_number = registration_for(entry.name)
    cursor.execute("select id from members where registration_number = %s", (registration_number,))
    row = cursor.fetchone()

    if row:
        return int(row[0]), False

    cursor.execute("select id, full_name, `rank` from members")
    for member_id, full_name, rank in cursor.fetchall():
        if normalized_identity(full_name) == normalized_identity(entry.name) and ranks_are_compatible(rank, entry.rank):
            return int(member_id), False

    cursor.execute(
        """
        insert into members
            (full_name, registration_number, `rank`, unit, role, status, created_at, updated_at)
        values
            (%s, %s, %s, 'DGP', %s, 'ativo', now(), now())
        """,
        (
            entry.name,
            registration_number,
            entry.rank,
            "Importado do financeiro",
        ),
    )
    return int(cursor.lastrowid), True


def import_entries(entries: list[FinancialEntry], reference_month: str) -> dict[str, int]:
    created_members = 0
    created_payments = 0
    updated_payments = 0

    with connect() as conn:
        cursor = conn.cursor()

        for entry in entries:
            member_id, member_created = upsert_member(cursor, entry)
            created_members += 1 if member_created else 0

            cursor.execute("select id from financial_payments where source_key = %s", (entry.source_key,))
            payment_row = cursor.fetchone()
            values = (
                member_id,
                reference_month,
                "beneficio" if entry.category == "Administrativo" else "gratificacao",
                entry.category,
                entry.department,
                entry.name,
                entry.total_minutes,
                entry.extra_minutes,
                entry.function_count,
                entry.functions_label,
                entry.amount,
                0,
                entry.amount,
                "pago",
                reference_month,
                "Importado do arquivo financeiro via Python.",
            )

            if payment_row:
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
                    values + (entry.source_key,),
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
                    (entry.source_key,) + values,
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
            insert ignore into sectors (name, description, status, created_at, updated_at)
            values (%s, 'Policiais sem setor administrativo definido.', 'ativo', now(), now())
            """,
            (NO_ADMIN_SECTOR,),
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
        conn.commit()

    return {
        "created_members": created_members,
        "created_payments": created_payments,
        "updated_payments": updated_payments,
        "total_entries": len(entries),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Inicializa o banco do DGP e importa os dados financeiros.")
    parser.add_argument("--file", default=DEFAULT_DATA_FILE, help="Arquivo de origem dos pagamentos.")
    parser.add_argument("--reference-month", default="2026-04-01", help="Mes de referencia no formato YYYY-MM-DD.")
    parser.add_argument("--reset", action="store_true", help="Apaga e recria as tabelas antes de importar.")
    args = parser.parse_args()

    create_database()
    create_schema(reset=args.reset)
    entries = parse_file(args.file)
    result = import_entries(entries, args.reference_month)

    print("Banco pronto.")
    print(f"Arquivo: {args.file}")
    print(f"Registros no arquivo: {result['total_entries']}")
    print(f"Membros criados: {result['created_members']}")
    print(f"Pagamentos criados: {result['created_payments']}")
    print(f"Pagamentos atualizados: {result['updated_payments']}")


if __name__ == "__main__":
    main()
