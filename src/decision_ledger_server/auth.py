"""API Key and Multi-Tenant Authentication Manager for Decision Ledger Control Plane."""

from __future__ import annotations

import hashlib
import hmac
import secrets
from typing import Dict, Optional


class AuthManager:
    """Manages API Keys and maps them securely to Organization and Tenant IDs."""

    def __init__(self) -> None:
        # In-memory store mapping hashed_key -> {org_id, tenant_id, name}
        self._keys: Dict[str, Dict[str, str]] = {}

    def generate_key(self, org_id: str, tenant_id: str = "default", name: str = "default-key") -> str:
        """Generate a new secure API key and register it for a tenant."""
        raw_key = f"dl_live_{secrets.token_urlsafe(32)}"
        key_hash = self._hash_key(raw_key)
        self._keys[key_hash] = {
            "org_id": org_id,
            "tenant_id": tenant_id,
            "name": name,
        }
        return raw_key

    def register_key(self, raw_key: str, org_id: str, tenant_id: str = "default") -> None:
        """Register an existing key string."""
        key_hash = self._hash_key(raw_key)
        self._keys[key_hash] = {
            "org_id": org_id,
            "tenant_id": tenant_id,
            "name": "registered-key",
        }

    def authenticate(self, raw_key: Optional[str]) -> Optional[Dict[str, str]]:
        """Validate API key and return tenant context dictionary if valid."""
        if not raw_key:
            return None
        key_hash = self._hash_key(raw_key)
        return self._keys.get(key_hash)

    @staticmethod
    def _hash_key(raw_key: str) -> str:
        """SHA-256 hash for secure key storage and lookup."""
        return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
