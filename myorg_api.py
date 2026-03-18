"""
myorg_api.py — Clarivate MyOrg REST API client
================================================
Wraps the MyOrg Swagger API to support the upload workflow:

  1. Check whether a (PersonID, DocumentID) pair already exists
     → POST /publications  (409 = already exists)
  2. Add a new person + org association
     → POST /persons
  3. Link an existing person-org to a publication
     → POST /publications  (or the dedicated endpoint)

All calls are synchronous, one entry at a time for maximum observability.
Batch helpers are also provided (up to 25 pubs / 50 persons per call).

API base: https://api.clarivate.com/api/myorg
Auth:     X-ApiKey header
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

import requests

BASE_URL = "https://api.clarivate.com/api/myorg"
TIMEOUT  = 20   # seconds per request


# ── Result types ──────────────────────────────────────────────────────────────

@dataclass
class ApiResult:
    success:    bool
    status:     int           # HTTP status code
    message:    str           # human-readable outcome
    payload:    dict = field(default_factory=dict)  # raw JSON response


# ── Client ────────────────────────────────────────────────────────────────────

class MyOrgClient:
    """
    Thin wrapper around the MyOrg REST API.

    Parameters
    ----------
    api_key : str
        Your Clarivate API key (passed as X-ApiKey header).
    dry_run : bool
        When True, every method logs what it *would* do and returns a
        synthetic success result without making any real HTTP calls.
        Useful for testing the upload logic before going live.
    """

    def __init__(self, api_key: str, dry_run: bool = False):
        self.api_key = api_key.strip()
        self.dry_run = dry_run
        self._session = requests.Session()
        self._session.headers.update({
            "X-ApiKey":     self.api_key,
            "Content-Type": "application/json",
            "Accept":       "application/json",
        })

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _get(self, path: str) -> requests.Response:
        return self._session.get(f"{BASE_URL}{path}", timeout=TIMEOUT)

    def _post(self, path: str, body) -> requests.Response:
        return self._session.post(f"{BASE_URL}{path}", json=body, timeout=TIMEOUT)

    def _delete(self, path: str) -> requests.Response:
        return self._session.delete(f"{BASE_URL}{path}", timeout=TIMEOUT)

    @staticmethod
    def _result(resp: requests.Response, ok_codes=(200, 201, 204)) -> ApiResult:
        payload = {}
        try:
            payload = resp.json()
        except Exception:
            pass
        success = resp.status_code in ok_codes
        return ApiResult(
            success=success,
            status=resp.status_code,
            message=str(payload) if payload else resp.reason or str(resp.status_code),
            payload=payload if isinstance(payload, dict) else {"data": payload},
        )

    # ── Person management ─────────────────────────────────────────────────────

    def add_person(
        self,
        person_id:    str,
        first_name:   str,
        last_name:    str,
        org_id:       str,
        email:        str = "",
        other_names:  str = "",
    ) -> ApiResult:
        """
        POST /persons
        Add a new person and associate them to an organisation.
        Returns success on 201; 409 means the person already exists.
        """
        if self.dry_run:
            return ApiResult(True, 201,
                f"[DRY RUN] Would add person {person_id} ({last_name}, {first_name}) "
                f"→ org {org_id}")

        body = [{
            "personId":   person_id,
            "firstName":  first_name,
            "lastName":   last_name,
            "email":      email,
            "otherNames": other_names,
            "organizations": [{"organizationId": org_id}],
        }]
        try:
            resp = self._post("/persons", body)
            result = self._result(resp, ok_codes=(200, 201))
            if resp.status_code == 409:
                result.message = "Person already exists (409)"
            return result
        except requests.RequestException as exc:
            return ApiResult(False, 0, f"Network error: {exc}")

    def associate_person_org(self, person_id: str, org_id: str) -> ApiResult:
        """
        POST /organizations/{orgId}/persons/{personId}
        Associate an existing person to an additional organisation.
        """
        if self.dry_run:
            return ApiResult(True, 201,
                f"[DRY RUN] Would associate person {person_id} → org {org_id}")
        try:
            resp = self._post(f"/organizations/{org_id}/persons/{person_id}", {})
            result = self._result(resp, ok_codes=(200, 201))
            if resp.status_code == 409:
                result.message = "Association already exists (409)"
            return result
        except requests.RequestException as exc:
            return ApiResult(False, 0, f"Network error: {exc}")

    # ── Publication management ─────────────────────────────────────────────────

    def add_publication(
        self,
        doc_id:     str,   # WOS ID (e.g. "WOS:001234567800001")
        person_id:  str,
        org_id:     str,
    ) -> ApiResult:
        """
        POST /publications
        Link a WoS document to a person-organisation pair.
        Returns 200 on success, 409 if already linked.
        """
        if self.dry_run:
            return ApiResult(True, 200,
                f"[DRY RUN] Would link {doc_id} → person {person_id} / org {org_id}")

        body = [{
            "docId": doc_id,
            "persons": [{"personId": person_id, "organizationId": org_id}],
        }]
        try:
            resp = self._post("/publications", body)
            # The publications endpoint returns 200 always; check individual status
            if resp.status_code == 200:
                try:
                    items = resp.json()
                    if isinstance(items, list) and items:
                        item = items[0]
                        # Check per-item status
                        persons = item.get("persons", [])
                        if persons and persons[0].get("error"):
                            err = persons[0]["error"]
                            already = "already" in err.lower() or "409" in str(err)
                            return ApiResult(
                                success=not already or False,
                                status=409 if already else 400,
                                message=err,
                                payload=item,
                            )
                        if item.get("error"):
                            return ApiResult(False, 400, item["error"], item)
                        return ApiResult(True, 200, "Linked successfully", item)
                except Exception:
                    pass
                return ApiResult(True, 200, "Linked successfully")
            result = self._result(resp, ok_codes=(200,))
            if resp.status_code == 409:
                result.message = "Publication already linked (409)"
            return result
        except requests.RequestException as exc:
            return ApiResult(False, 0, f"Network error: {exc}")

    def link_existing_publication(
        self,
        doc_id:    str,
        person_id: str,
        org_id:    str,
    ) -> ApiResult:
        """
        POST /organizations/{orgId}/persons/{personId}/publications/{pubId}
        Associate an existing publication to an existing person-org pair.
        """
        if self.dry_run:
            return ApiResult(True, 201,
                f"[DRY RUN] Would link existing pub {doc_id} → {person_id}/{org_id}")
        try:
            resp = self._post(
                f"/organizations/{org_id}/persons/{person_id}/publications/{doc_id}",
                {},
            )
            result = self._result(resp, ok_codes=(200, 201))
            if resp.status_code == 409:
                result.message = "Already linked (409)"
            return result
        except requests.RequestException as exc:
            return ApiResult(False, 0, f"Network error: {exc}")

    # ── High-level upload flow ────────────────────────────────────────────────

    def upload_row(
        self,
        row:       dict,
        is_new_person: bool = False,
        first_name: str = "",
        last_name:  str = "",
        delay:     float = 0.3,
    ) -> dict:
        """
        Upload a single output row (PersonID, OrganizationID, UT, AuthorFullName).

        Strategy
        --------
        1. If is_new_person → POST /persons to create and associate
        2. Then POST /publications to link the document

        Returns a status dict with keys:
          person_step : ApiResult
          pub_step    : ApiResult
          overall     : 'ok' | 'skipped' | 'error'
        """
        pid = str(row.get("PersonID", "")).strip()
        oid = str(row.get("OrganizationID", "")).strip()
        ut  = str(row.get("UT", row.get("DocumentID", ""))).strip()
        full_name = row.get("AuthorFullName", "")

        # Split name if first/last not provided
        if not first_name and not last_name:
            if "," in full_name:
                last_name, _, first_name = full_name.partition(",")
                first_name = first_name.strip()
                last_name  = last_name.strip()
            else:
                last_name  = full_name.strip()
                first_name = ""

        result = {"person_step": None, "pub_step": None, "overall": "ok"}

        # ── Step 1: ensure person exists in the org ──────────────────────────
        if is_new_person:
            p_res = self.add_person(pid, first_name, last_name, oid)
            result["person_step"] = p_res
            if not p_res.success and p_res.status != 409:
                result["overall"] = "error"
                return result
        else:
            # Person exists — ensure org association exists
            p_res = self.associate_person_org(pid, oid)
            result["person_step"] = p_res
            # 409 = already associated, which is fine
            if not p_res.success and p_res.status != 409:
                result["overall"] = "error"
                return result

        if delay > 0 and not self.dry_run:
            time.sleep(delay)

        # ── Step 2: link the publication ─────────────────────────────────────
        pub_res = self.add_publication(ut, pid, oid)
        result["pub_step"] = pub_res

        if pub_res.status == 409:
            result["overall"] = "skipped"   # already linked
        elif not pub_res.success:
            result["overall"] = "error"

        return result

    def test_connection(self) -> ApiResult:
        """
        Quick connectivity check — attempts to POST an intentionally
        invalid person payload and expects a 400 (not a network error).
        A 400 means the API is reachable and the key is accepted.
        A 401/403 means bad API key.
        """
        if self.dry_run:
            return ApiResult(True, 200, "[DRY RUN] Connection test skipped")
        try:
            resp = self._post("/persons", [{"personId": "__test__"}])
            if resp.status_code in (400, 409):
                return ApiResult(True, resp.status_code,
                    "API reachable — key accepted (got expected 400/409 on test payload)")
            if resp.status_code == 401:
                return ApiResult(False, 401, "Invalid API key (401 Unauthorized)")
            if resp.status_code == 403:
                return ApiResult(False, 403, "API key forbidden (403)")
            return ApiResult(True, resp.status_code,
                f"API reachable (status {resp.status_code})")
        except requests.ConnectionError:
            return ApiResult(False, 0,
                "Cannot reach api.clarivate.com — check network/VPN")
        except requests.Timeout:
            return ApiResult(False, 0, "Request timed out")
        except requests.RequestException as exc:
            return ApiResult(False, 0, f"Network error: {exc}")
