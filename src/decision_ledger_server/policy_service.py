"""Centralized Policy Generator & Distribution Service for SaaS Control Plane."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, Optional

from decision_ledger.policy import policy_from_dict

logger = logging.getLogger(__name__)


class CentralPolicyService:
    """Manages active serving policies per tenant and generates versioned artifacts."""

    def __init__(self) -> None:
        # tenant_id -> active policy dictionary
        self._policies: Dict[str, Dict[str, Any]] = {}

    def set_policy(self, tenant_id: str, policy_dict: Dict[str, Any]) -> None:
        """Register or update active policy dict for a tenant."""
        if "policy_version" not in policy_dict:
            policy_dict["policy_version"] = f"saas-{tenant_id}-{int(time.time())}"
        self._policies[tenant_id] = policy_dict
        logger.info("Updated SaaS policy for tenant %s -> version %s", tenant_id, policy_dict["policy_version"])

    def get_policy(self, tenant_id: str) -> Optional[Dict[str, Any]]:
        """Get active policy dict for a tenant."""
        return self._policies.get(tenant_id)
