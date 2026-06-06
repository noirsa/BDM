from __future__ import annotations

import os
import logging
import time

from pymilvus import MilvusClient


logging.getLogger("pymilvus").setLevel(logging.CRITICAL)

MILVUS_URI = os.getenv("MILVUS_URI", "http://milvus:19530")
ROOT_USER = os.getenv("MILVUS_ROOT_USER", "root")
ROOT_PASSWORD = os.getenv("MILVUS_ROOT_PASSWORD", "Milvus")
WRITER_USER = os.getenv("MILVUS_WRITER_USER", "bdm_vector_writer")
WRITER_PASSWORD = os.getenv("MILVUS_WRITER_PASSWORD", "bdm_vector_writer_password")
READER_USER = os.getenv("MILVUS_READER_USER", "bdm_vector_reader")
READER_PASSWORD = os.getenv("MILVUS_READER_PASSWORD", "bdm_vector_reader_password")
COLLECTION_NAME = os.getenv("MILVUS_IMAGE_COLLECTION", "image_vector_catalog")
USE_PRIVILEGE_GROUPS = os.getenv("MILVUS_USE_PRIVILEGE_GROUPS", "").lower() in {"1", "true", "yes"}


def root_client() -> MilvusClient:
    return MilvusClient(uri=MILVUS_URI, user=ROOT_USER, password=ROOT_PASSWORD)


def wait_for_milvus() -> MilvusClient:
    last_error: Exception | None = None
    for _ in range(60):
        try:
            client = root_client()
            client.list_collections()
            return client
        except Exception as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"Milvus did not become ready: {last_error}")


def ensure_user(client: MilvusClient, user_name: str, password: str) -> None:
    try:
        users = set(client.list_users())
    except Exception:
        users = set()
    if user_name not in users:
        client.create_user(user_name=user_name, password=password)
        print(f"Created Milvus user {user_name}")
    else:
        try:
            client.update_password(user_name=user_name, old_password=password, new_password=password)
        except Exception:
            print(f"Milvus user {user_name} already exists")


def ensure_role(client: MilvusClient, role_name: str, user_name: str) -> None:
    try:
        roles = set(client.list_roles())
    except Exception:
        roles = set()
    if role_name not in roles:
        client.create_role(role_name=role_name)
        print(f"Created Milvus role {role_name}")
    try:
        client.grant_role(user_name=user_name, role_name=role_name)
    except Exception as exc:
        print(f"Milvus role {role_name} already attached to {user_name} or attach skipped: {exc}")


def grant_privilege_group(client: MilvusClient, role_name: str, privilege_group: str) -> bool:
    if not USE_PRIVILEGE_GROUPS or not hasattr(client, "grant_privilege_v2"):
        return False
    for kwargs in (
        {"role_name": role_name, "privilege": privilege_group, "collection_name": "*", "db_name": "default"},
        {"role_name": role_name, "privilege": privilege_group, "collection_name": COLLECTION_NAME, "db_name": "default"},
    ):
        try:
            client.grant_privilege_v2(**kwargs)
            print(f"Granted {privilege_group} to {role_name}")
            return True
        except Exception as exc:
            print(f"Could not grant {privilege_group} to {role_name} with v2 API: {exc}")
    return False


def grant_privileges(client: MilvusClient, role_name: str, privileges: list[str]) -> None:
    for privilege in privileges:
        granted = False
        object_types = ["Global"] if privilege in {"CreateCollection", "DropCollection"} else ["Collection", "Global"]
        for object_type in object_types:
            for object_name in (COLLECTION_NAME, "*"):
                try:
                    client.grant_privilege(
                        role_name=role_name,
                        object_type=object_type,
                        object_name=object_name,
                        privilege=privilege,
                        db_name="default",
                    )
                    granted = True
                    break
                except Exception:
                    continue
            if granted:
                break
        if granted:
            print(f"Granted {privilege} to {role_name}")
        else:
            print(f"Could not grant {privilege} to {role_name}; it may already exist or be unsupported by this Milvus version")


def grant_legacy_reader_privileges(client: MilvusClient, role_name: str) -> None:
    for privilege in ("Load", "Search", "Query", "GetStatistics"):
        granted = False
        for object_name in (COLLECTION_NAME, "*"):
            try:
                client.grant_privilege(
                    role_name=role_name,
                    object_type="Collection",
                    object_name=object_name,
                    privilege=privilege,
                    db_name="default",
                )
                granted = True
                break
            except Exception:
                continue
        if granted:
            print(f"Granted {privilege} to {role_name}")
        else:
            print(f"Could not grant {privilege} to {role_name}; it may already exist or be unsupported by this Milvus version")


def main() -> None:
    client = wait_for_milvus()
    ensure_user(client, WRITER_USER, WRITER_PASSWORD)
    ensure_user(client, READER_USER, READER_PASSWORD)
    ensure_role(client, "bdm_vector_writer_role", WRITER_USER)
    ensure_role(client, "bdm_vector_reader_role", READER_USER)
    if not grant_privilege_group(client, "bdm_vector_writer_role", "COLL_ADMIN"):
        grant_privileges(
            client,
            "bdm_vector_writer_role",
            ["CreateCollection", "DropCollection", "Load", "Insert", "Upsert", "Flush", "Search", "Query", "ShowCollections", "GetStatistics", "CreateIndex"],
        )
    if not grant_privilege_group(client, "bdm_vector_reader_role", "COLL_RO"):
        grant_legacy_reader_privileges(client, "bdm_vector_reader_role")
    print("Milvus RBAC bootstrap completed. Root account is preserved.")


if __name__ == "__main__":
    main()
