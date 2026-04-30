from __future__ import annotations

import argparse
import difflib
import json
import os
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import mysql.connector


ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_FILE = Path.home() / "Desktop" / "backup_registros_1777325803764.json"


@dataclass(frozen=True)
class BackupEntry:
    name: str
    rg: str
    rank: str
    rank_key: str


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


def rank_key(raw_rank: str | None) -> str:
    mapping = {
        "t coronel": "tcoronel",
        "coronel": "coronel",
        "major": "major",
        "capitao": "capitao",
        "2 tenente": "2tenente",
        "1 tenente": "1tenente",
        "s tenente": "stenente",
        "aspirante": "aspirante",
        "1 sargento": "1sargento",
        "2 sargento": "2sargento",
        "3 sargento": "3sargento",
        "cabo": "cabo",
        "sd 1a cl": "sd1",
    }
    return mapping.get(normalize_text(raw_rank), "outros")


def load_backup(path: Path) -> dict[str, list[BackupEntry]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    index: dict[str, list[BackupEntry]] = {}
    for item in raw:
        name = str(item.get("nome") or "").strip()
        rg = str(item.get("rg") or "").strip()
        rank = str(item.get("patente") or "").strip()
        if not name or not rg:
            continue
        key = simplify_name(name)
        index.setdefault(key, []).append(
            BackupEntry(
                name=name,
                rg=rg,
                rank=rank,
                rank_key=rank_key(rank),
            )
        )
    return index


def choose_candidate(member_name: str, member_rank: str, candidates: list[BackupEntry]) -> BackupEntry | None:
    if not candidates:
        return None
    unique_rgs = {candidate.rg for candidate in candidates}
    if len(unique_rgs) == 1:
        return candidates[0]

    target_rank = rank_key(member_rank)
    ranked = [candidate for candidate in candidates if candidate.rank_key == target_rank]
    ranked_unique = {candidate.rg for candidate in ranked}
    if len(ranked_unique) == 1:
        return ranked[0]
    return None


def choose_fuzzy_candidate(member_name: str, member_rank: str, all_entries: list[BackupEntry]) -> BackupEntry | None:
    member_simple = simplify_name(member_name)
    target_rank = rank_key(member_rank)
    scored: list[tuple[float, BackupEntry]] = []

    for entry in all_entries:
        if not member_simple or not simplify_name(entry.name):
            continue
        if not (target_rank == entry.rank_key or target_rank == "outros" or entry.rank_key == "outros"):
            continue
        score = difflib.SequenceMatcher(None, member_simple, simplify_name(entry.name)).ratio()
        if member_simple in simplify_name(entry.name) or simplify_name(entry.name) in member_simple:
            score = max(score, 0.92)
        if score >= 0.90:
            scored.append((score, entry))

    scored.sort(key=lambda item: item[0], reverse=True)
    unique_by_rg: list[tuple[float, BackupEntry]] = []
    seen_rgs: set[str] = set()
    for score, entry in scored:
        if entry.rg in seen_rgs:
            continue
        seen_rgs.add(entry.rg)
        unique_by_rg.append((score, entry))

    if len(unique_by_rg) == 1 and unique_by_rg[0][0] >= 0.92:
        return unique_by_rg[0][1]
    return None


def merge_into_canonical(cursor, imp_id: int, canonical_id: int) -> None:
    cursor.execute(
        """
        update financial_payments
        set member_id = %s,
            updated_at = now()
        where member_id = %s
        """,
        (canonical_id, imp_id),
    )
    cursor.execute(
        """
        insert ignore into member_sectors (member_id, sector_id, created_at)
        select %s, sector_id, now()
        from member_sectors
        where member_id = %s
        """,
        (canonical_id, imp_id),
    )
    cursor.execute("delete from member_sectors where member_id = %s", (imp_id,))
    cursor.execute("delete from members where id = %s", (imp_id,))


def main() -> int:
    parser = argparse.ArgumentParser(description="Ajusta matriculas IMP-* com RG do backup.")
    parser.add_argument("--file", default=str(DEFAULT_FILE), help="Arquivo JSON de backup.")
    parser.add_argument("--dry-run", action="store_true", help="Simula sem gravar.")
    args = parser.parse_args()

    load_env_file(ROOT_DIR / ".env")
    backup_file = Path(args.file)
    if not backup_file.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {backup_file}")

    backup_index = load_backup(backup_file)
    all_backup_entries = [entry for values in backup_index.values() for entry in values]

    matched = 0
    no_match = 0
    ambiguous = 0
    fuzzy_matched = 0
    merged_deleted = 0
    updated_in_place = 0
    already_ok = 0

    with connect() as conn:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            """
            select id, full_name, registration_number, `rank`
            from members
            where registration_number like 'IMP-%'
            order by id
            """
        )
        imp_rows = cursor.fetchall()

        for member in imp_rows:
            key = simplify_name(member.get("full_name"))
            candidates = backup_index.get(key, [])
            candidate = choose_candidate(member.get("full_name", ""), member.get("rank", ""), candidates)
            if not candidates:
                candidate = choose_fuzzy_candidate(member.get("full_name", ""), member.get("rank", ""), all_backup_entries)
                if not candidate:
                    no_match += 1
                    continue
                fuzzy_matched += 1
            if not candidate:
                ambiguous += 1
                continue

            matched += 1
            cursor.execute(
                "select id from members where registration_number = %s limit 1",
                (candidate.rg,),
            )
            canonical = cursor.fetchone()
            if canonical and int(canonical["id"]) != int(member["id"]):
                merge_into_canonical(cursor, int(member["id"]), int(canonical["id"]))
                merged_deleted += 1
            elif canonical and int(canonical["id"]) == int(member["id"]):
                already_ok += 1
            else:
                cursor.execute(
                    """
                    update members
                    set registration_number = %s,
                        updated_at = now()
                    where id = %s
                    """,
                    (candidate.rg, int(member["id"])),
                )
                updated_in_place += 1

        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()

    print(f"Arquivo backup: {backup_file}")
    print(f"Total IMP analisados: {len(imp_rows)}")
    print(f"Com match seguro: {matched}")
    print(f"Match por fuzzy de alta confianca: {fuzzy_matched}")
    print(f"Sem match no backup: {no_match}")
    print(f"Ambiguos: {ambiguous}")
    print(f"Mesclados/removidos IMP: {merged_deleted}")
    print(f"Atualizados in-place para RG: {updated_in_place}")
    print(f"Ja apontavam para o proprio RG: {already_ok}")
    print("Modo: DRY-RUN" if args.dry_run else "Modo: APPLY")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
