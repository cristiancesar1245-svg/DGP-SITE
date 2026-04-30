from __future__ import annotations

import argparse
import json
import os
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import mysql.connector


ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_FILE = Path.home() / "Desktop" / "backup_registros_1777325803764.json"
NO_ADMIN_SECTOR = "SEM SETOR ADM"


@dataclass(frozen=True)
class BackupMember:
    name: str
    rg: str
    rank: str
    raw_status: str


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value or value.startswith("#") or "=" not in value:
            continue
        key, env_value = value.split("=", 1)
        os.environ.setdefault(key.strip(), env_value.strip().strip('"').strip("'"))


def connect():
    return mysql.connector.connect(
        host=os.getenv("DGP_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DGP_DB_PORT", "3308")),
        database=os.getenv("DGP_DB_DATABASE", "dgp"),
        user=os.getenv("DGP_DB_USERNAME", "root"),
        password=os.getenv("DGP_DB_PASSWORD", "12457803"),
        charset="utf8mb4",
    )


def normalize_text(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def simplify_name(value: str | None) -> str:
    normalized = normalize_text(value)
    for token in (
        "t coronel",
        "coronel",
        "major",
        "capitao",
        "1 tenente",
        "2 tenente",
        "s tenente",
        "aspirante",
        "1 sargento",
        "2 sargento",
        "3 sargento",
        "cabo",
        "sd 1a cl",
        "sd 1 cl",
    ):
        normalized = re.sub(rf"\b{re.escape(token)}\b", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def normalize_rank(raw_rank: str | None) -> str:
    rank = (raw_rank or "").strip()
    if not rank:
        return "Outros"
    key = normalize_text(rank)
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
    return mapping.get(key, rank)


def status_from_backup(raw_status: str | None) -> str:
    if normalize_text(raw_status) == "aprovado":
        return "ativo"
    return "inativo"


def load_backup(path: Path) -> list[BackupMember]:
    data = json.loads(path.read_text(encoding="utf-8"))
    members: list[BackupMember] = []
    for item in data:
        name = str(item.get("nome") or "").strip()
        rg = str(item.get("rg") or "").strip()
        if not name or not rg:
            continue
        members.append(
            BackupMember(
                name=name,
                rg=rg,
                rank=normalize_rank(item.get("patente")),
                raw_status=str(item.get("status") or "").strip(),
            )
        )
    return members


def ensure_no_admin_sector(cursor) -> int:
    cursor.execute(
        """
        insert ignore into sectors (name, description, status, created_at, updated_at)
        values (%s, 'Policiais sem setor administrativo definido.', 'ativo', now(), now())
        """,
        (NO_ADMIN_SECTOR,),
    )
    cursor.execute("select id from sectors where name = %s", (NO_ADMIN_SECTOR,))
    row = cursor.fetchone()
    return int(row[0])


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sincroniza tabela members com backup de registros (matricula = rg)."
    )
    parser.add_argument("--file", default=str(DEFAULT_FILE), help="Arquivo JSON de backup.")
    parser.add_argument("--dry-run", action="store_true", help="Simula sem gravar no banco.")
    args = parser.parse_args()

    load_env_file(ROOT_DIR / ".env")
    backup_file = Path(args.file)
    if not backup_file.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {backup_file}")

    backup_members = load_backup(backup_file)
    if not backup_members:
        print("Nenhum membro valido no arquivo.")
        return 0

    backup_name_counts = Counter(simplify_name(member.name) for member in backup_members)
    backup_rg_set = {member.rg for member in backup_members}

    created = 0
    updated_by_rg = 0
    updated_by_name = 0
    ambiguous_inserted = 0
    skipped_invalid = 0
    reactivated = 0
    unchanged = 0

    with connect() as conn:
        cursor = conn.cursor()
        no_admin_sector_id = ensure_no_admin_sector(cursor)

        cursor.execute(
            """
            select id, full_name, registration_number, `rank`, unit, role, status
            from members
            """
        )
        rows = cursor.fetchall()

        by_rg: dict[str, dict] = {}
        by_name: dict[str, list[dict]] = {}
        for row in rows:
            member = {
                "id": int(row[0]),
                "full_name": row[1] or "",
                "registration_number": row[2] or "",
                "rank": row[3] or "",
                "unit": row[4] or "",
                "role": row[5] or "",
                "status": row[6] or "",
            }
            reg = member["registration_number"].strip()
            if reg:
                by_rg[reg] = member
            key = simplify_name(member["full_name"])
            if key:
                by_name.setdefault(key, []).append(member)

        for incoming in backup_members:
            target_status = status_from_backup(incoming.raw_status)
            existing = by_rg.get(incoming.rg)
            if existing:
                changed = False
                if existing["full_name"] != incoming.name:
                    changed = True
                if (existing["rank"] or "") != incoming.rank:
                    changed = True
                if (existing["status"] or "") != target_status:
                    changed = True
                if changed:
                    cursor.execute(
                        """
                        update members
                        set full_name = %s,
                            `rank` = %s,
                            status = %s,
                            updated_at = now()
                        where id = %s
                        """,
                        (incoming.name, incoming.rank, target_status, existing["id"]),
                    )
                    updated_by_rg += 1
                    if existing.get("status") != "ativo" and target_status == "ativo":
                        reactivated += 1
                else:
                    unchanged += 1
                continue

            key = simplify_name(incoming.name)
            candidates = by_name.get(key, []) if key else []
            if key and backup_name_counts.get(key, 0) == 1 and len(candidates) == 1:
                target = candidates[0]
                if target.get("registration_number", "").strip() and target["registration_number"] not in backup_rg_set:
                    cursor.execute(
                        """
                        update members
                        set registration_number = %s,
                            full_name = %s,
                            `rank` = %s,
                            status = %s,
                            updated_at = now()
                        where id = %s
                        """,
                        (incoming.rg, incoming.name, incoming.rank, target_status, target["id"]),
                    )
                    updated_by_name += 1
                    if target.get("status") != "ativo" and target_status == "ativo":
                        reactivated += 1
                    continue
                else:
                    ambiguous_inserted += 1
            elif key and (backup_name_counts.get(key, 0) > 1 or len(candidates) > 1):
                ambiguous_inserted += 1

            unit = NO_ADMIN_SECTOR
            role = "Membro"
            cursor.execute(
                """
                insert into members
                    (full_name, registration_number, `rank`, unit, role, status, created_at, updated_at)
                values
                    (%s, %s, %s, %s, %s, %s, now(), now())
                """,
                (incoming.name, incoming.rg, incoming.rank, unit, role, target_status),
            )
            member_id = int(cursor.lastrowid)
            cursor.execute(
                """
                insert ignore into member_sectors (member_id, sector_id, created_at)
                values (%s, %s, now())
                """,
                (member_id, no_admin_sector_id),
            )
            created += 1

        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()

    print(f"Arquivo: {backup_file}")
    print(f"Registros validos no backup: {len(backup_members)}")
    print(f"Criados: {created}")
    print(f"Atualizados por RG: {updated_by_rg}")
    print(f"Atualizados por nome: {updated_by_name}")
    print(f"Reativados: {reactivated}")
    print(f"Sem alteracao: {unchanged}")
    print(f"Ambiguos inseridos como novo registro: {ambiguous_inserted}")
    print(f"Ignorados por invalidez: {skipped_invalid}")
    print("Modo: DRY-RUN (sem gravacao)" if args.dry_run else "Modo: APPLY (gravado no banco)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
