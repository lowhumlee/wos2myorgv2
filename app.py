"""
app.py — WoS MyOrg Affiliation Tool v2
UT-centric review with inline API upload after each publication is confirmed.
Medical University of Varna · Research Information Systems
"""

from __future__ import annotations

import re
from datetime import datetime

import pandas as pd
import streamlit as st

from myorg_api import MyOrgClient
from core import (
    load_config,
    build_person_index,
    parse_org_hierarchy,
    parse_wos_csv,
    extract_muv_author_pairs,
    batch_process,
    normalize_name,
    name_similarity,
)

st.set_page_config(page_title="WoS → MyOrg  v2", page_icon="🔬",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');
html,body,[class*="css"]{font-family:'IBM Plex Sans',sans-serif;}
.app-header{background:#0f1923;color:#e8f4f8;padding:1.4rem 2rem 1.2rem;border-bottom:3px solid #1a9dc8;margin:-1rem -1rem 1.5rem;display:flex;align-items:baseline;gap:1rem;}
.app-header h1{font-size:1.4rem;font-weight:600;margin:0;color:#fff;}
.app-header .sub{font-family:'IBM Plex Mono',monospace;font-size:.75rem;color:#7ec8e3;}
.ut-card{background:#0f1923;border:1px solid #1a4a6b;border-left:4px solid #1a9dc8;border-radius:6px;padding:1rem 1.4rem;margin-bottom:1.2rem;font-family:'IBM Plex Mono',monospace;}
.ut-card .ut-id{font-size:1.05rem;font-weight:600;color:#7ec8e3;}
.ut-card .ut-meta{font-size:.78rem;color:#8aabb8;margin-top:.3rem;}
.author-row{border:1px solid #e0e8ef;border-radius:6px;padding:.85rem 1rem;margin-bottom:.7rem;background:#fafcfe;}
.author-row.locked{background:#f0faf4;border-color:#27ae60;}
.author-row.rejected{background:#fff5f5;border-color:#e74c3c;}
.badge{display:inline-block;padding:.15rem .55rem;border-radius:3px;font-size:.72rem;font-weight:600;letter-spacing:.04em;font-family:'IBM Plex Mono',monospace;margin-right:.4rem;}
.badge-exact{background:#d4edda;color:#155724;}.badge-initial{background:#fff3cd;color:#856404;}
.badge-fuzzy{background:#d1ecf1;color:#0c5460;}.badge-new{background:#e8d5f5;color:#5a1f8a;}
.badge-skip{background:#e2e3e5;color:#383d41;}.badge-dup{background:#f8d7da;color:#721c24;}
.prog-bar-wrap{background:#e0e8ef;border-radius:4px;height:8px;margin:.5rem 0 1rem;}
.prog-bar-fill{background:linear-gradient(90deg,#1a9dc8,#27ae60);border-radius:4px;height:8px;transition:width .3s;}
.sec-head{font-size:.7rem;font-weight:600;letter-spacing:.1em;color:#1a9dc8;text-transform:uppercase;margin:1.2rem 0 .5rem;border-bottom:1px solid #d0e8f0;padding-bottom:.3rem;}
.chip{display:inline-block;background:#e8f4f8;color:#0c5460;border-radius:3px;padding:.1rem .45rem;font-size:.72rem;margin:.1rem;font-family:'IBM Plex Mono',monospace;border:1px solid #b8dde8;}
.locked-ut{display:inline-flex;align-items:center;gap:.5rem;background:#f0faf4;border:1px solid #27ae60;border-radius:4px;padding:.35rem .8rem;font-size:.82rem;color:#155724;font-weight:500;margin-bottom:.5rem;}
.metric-grid{display:flex;gap:.8rem;flex-wrap:wrap;margin:.8rem 0 1.2rem;}
.metric-card{background:#fff;border:1px solid #d0e8f0;border-radius:6px;padding:.7rem 1.1rem;min-width:110px;text-align:center;}
.metric-card .num{font-size:1.6rem;font-weight:700;font-family:'IBM Plex Mono',monospace;color:#0f1923;}
.metric-card .num-blue{color:#1a9dc8;}.metric-card .num-green{color:#27ae60;}
.metric-card .num-amber{color:#e67e22;}.metric-card .num-red{color:#e74c3c;}
.metric-card .lbl{font-size:.68rem;color:#7a8fa0;text-transform:uppercase;letter-spacing:.06em;margin-top:.15rem;}
.upl-table{width:100%;border-collapse:collapse;font-family:'IBM Plex Mono',monospace;font-size:.78rem;margin:.6rem 0 1rem;}
.upl-table th{background:#0f1923;color:#7ec8e3;padding:.45rem .7rem;text-align:left;font-weight:600;letter-spacing:.05em;}
.upl-table td{padding:.4rem .7rem;border-bottom:1px solid #e8f0f5;color:#2c3e50;}
.upl-table tr:hover td{background:#f5fafe;}
.upl-table .new-badge{color:#9b59b6;font-size:.7rem;font-weight:700;margin-left:.3rem;}
.upl-result{display:flex;align-items:center;gap:.6rem;padding:.35rem .7rem;border-radius:4px;margin:.15rem 0;font-size:.77rem;font-family:'IBM Plex Mono',monospace;}
.upl-result.ok{background:#f0faf4;}.upl-result.skip{background:#f5f5f5;color:#888;}.upl-result.error{background:#fff5f5;}
.r-pid{color:#1a9dc8;min-width:4rem;}.r-name{min-width:14rem;}.r-msg{color:#666;}
.api-bar{padding:.5rem 1rem;border-radius:5px;font-size:.8rem;margin-bottom:.8rem;display:flex;align-items:center;gap:.6rem;}
.api-ok{background:#f0faf4;border:1px solid #27ae60;color:#155724;}
.api-dry{background:#fff8e8;border:1px solid #e67e22;color:#8a5200;}
.api-off{background:#f8f8f8;border:1px solid #ccc;color:#666;}
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="app-header"><h1>🔬 WoS → MyOrg</h1>'
            '<span class="sub">v2 · UT-centric review + inline upload · Medical University of Varna</span>'
            '</div>', unsafe_allow_html=True)

# ── Session state ─────────────────────────────────────────────────────────────
# Load API key from config.json if present and not yet in session
_cfg_for_key = load_config("config.json")
_cfg_api_key = _cfg_for_key.get("api_key", "").strip()

for k, v in {
    "processed": False, "ut_order": [], "ut_index": 0,
    "ut_locked": {}, "author_decs": {}, "batch_result": None,
    "person_index": [], "existing_pairs": set(), "orgs": [], "cfg": {},
    "source_file": "", "max_pid": 0,
    "ut_rows_cache": {}, "ut_upload_done": {}, "upload_log": {},
    # Seed API key from config; dry_run=False when a key is configured
    "global_api_key": _cfg_api_key,
    "global_dry_run": not bool(_cfg_api_key),
}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Pure helpers ──────────────────────────────────────────────────────────────
def _safe_key(*parts):
    raw  = "_".join(str(p) for p in parts)
    safe = re.sub(r"[^a-z0-9]", "_", raw.lower())
    return re.sub(r"_+", "_", safe).strip("_")

def search_persons(query, person_index, max_results=8):
    if not query or len(query) < 2:
        return []
    q       = normalize_name(query)
    q_parts = q.split()
    results = []
    for p in person_index:
        name  = p["NormName"]
        score = name_similarity(q, name)
        if any(part in name for part in q_parts if len(part) > 2):
            score = max(score, 0.45)
        if score >= 0.28:
            results.append((score, p))
    results.sort(key=lambda x: -x[0])
    return results[:max_results]

def org_label(oid, org_map):
    for lbl, v in org_map.items():
        if v == oid:
            return lbl
    return oid

def build_org_map(orgs):
    m = {f"[{o['OrganizationID']}] {o['OrganizationName']}": o["OrganizationID"] for o in orgs}
    return m, ["— none / skip —"] + list(m.keys())

def _split_name(full_name):
    if "," in full_name:
        last, _, first = full_name.partition(",")
        return first.strip(), last.strip()
    return "", full_name.strip()

def _ut_status(ut):
    if st.session_state.ut_locked.get(ut):
        return "locked"
    res = st.session_state.batch_result
    if res is None:
        return "pending"
    nr = [r for r in res["needs_review"] if r.get("UT") == ut]
    cf = [r for r in res["confirmed"]    if r.get("UT") == ut]
    return "skip" if not nr and not cf else "pending"

def _ut_needs_attention(ut):
    res = st.session_state.batch_result
    return [] if res is None else [r for r in res["needs_review"] if r.get("UT") == ut]

def _ut_auto_confirmed(ut):
    res = st.session_state.batch_result
    return [] if res is None else [r for r in res["confirmed"] if r.get("UT") == ut]

def _ut_already_uploaded(ut):
    res = st.session_state.batch_result
    return [] if res is None else [r for r in res["already_uploaded"] if r.get("UT") == ut]

def _all_authors_decided(ut):
    for r in _ut_needs_attention(ut):
        dec = st.session_state.author_decs.get((normalize_name(r["AuthorFullName"]), ut))
        if not dec or not dec.get("decided"):
            return False
    return True

def _get_dec(norm, ut):
    return st.session_state.author_decs.get((norm, ut), {})

def _set_dec(norm, ut, dec):
    st.session_state.author_decs[(norm, ut)] = dec

def _ut_is_done(ut):
    if st.session_state.ut_locked.get(ut): return True
    if _ut_status(ut) == "skip":           return True
    if not _ut_needs_attention(ut):        return True
    return False

def _build_ut_rows(ut):
    """Build & cache upload-ready rows for one UT."""
    res = st.session_state.batch_result
    ep  = st.session_state.existing_pairs
    seen, output = set(), []

    for r in res["confirmed"]:
        if r.get("UT") != ut: continue
        pid, oid = r["PersonID"], r.get("OrganizationID", "")
        k = (pid, ut, oid)
        if k in seen: continue
        seen.add(k)
        first, last = _split_name(r.get("AuthorFullName", ""))
        output.append({"PersonID": pid, "FirstName": first, "LastName": last,
                       "OrganizationID": oid, "DocumentID": ut,
                       "AuthorFullName": r.get("AuthorFullName", ""),
                       "match_type": "exact", "is_new": False})

    for r in res["needs_review"]:
        if r.get("UT") != ut: continue
        raw   = r.get("AuthorFullName", r.get("author_full", ""))
        norm  = normalize_name(raw)
        dec   = st.session_state.author_decs.get((norm, ut))
        if not dec or dec.get("action") == "reject" or not dec.get("decided"): continue

        pid  = str(dec.get("resolved_pid", "")).strip()
        name = dec.get("resolved_name", raw)
        oids = [o for o in dec.get("org_ids", [""]) if o] or [""]

        if pid and (pid, ut) in ep: continue   # already in MyOrg

        is_new = dec.get("match_type") == "new"
        first, last = _split_name(name)
        for oid in oids:
            k = (pid, ut, oid)
            if k in seen: continue
            seen.add(k)
            output.append({"PersonID": pid, "FirstName": first, "LastName": last,
                           "OrganizationID": oid, "DocumentID": ut,
                           "AuthorFullName": name, "match_type": dec.get("match_type", ""),
                           "is_new": is_new})

    st.session_state.ut_rows_cache[ut] = output
    return output


def _show_ut_upload_section(ut):
    """Render upload table + upload button for a locked UT."""
    rows     = st.session_state.ut_rows_cache.get(ut) or _build_ut_rows(ut)
    api_key  = st.session_state.global_api_key
    dry_run  = st.session_state.global_dry_run
    uploaded = st.session_state.ut_upload_done.get(ut, False)
    log      = st.session_state.upload_log.get(ut, [])

    st.markdown('<div class="sec-head">Upload records</div>', unsafe_allow_html=True)

    if not rows:
        st.info("No records to upload — all authors excluded or already in MyOrg.")
        return

    # Table
    rows_html = "".join(
        f"<tr>"
        f"<td>{r['PersonID']}"
        f"{'<span class=\"new-badge\">NEW</span>' if r['is_new'] else ''}</td>"
        f"<td>{r['FirstName']}</td><td>{r['LastName']}</td>"
        f"<td>{r['OrganizationID']}</td><td>{r['DocumentID']}</td>"
        f"</tr>"
        for r in rows
    )
    st.markdown(f"""
<table class="upl-table">
<thead><tr><th>PersonID</th><th>FirstName</th><th>LastName</th>
<th>OrganizationID</th><th>DocumentID</th></tr></thead>
<tbody>{rows_html}</tbody>
</table>""", unsafe_allow_html=True)

    if not uploaded:
        if not api_key and not dry_run:
            st.warning("⚠️ Add your API key in the sidebar.")
            return
        lbl = f"🔬 Simulate upload ({len(rows)} rows)" if dry_run \
              else f"🚀 Upload {len(rows)} row(s) to MyOrg"
        if st.button(lbl, key=f"upload_ut_{_safe_key(ut)}", type="primary"):
            client  = MyOrgClient(api_key or "test", dry_run=dry_run)
            ut_log  = []
            prog    = st.progress(0)
            for i, row in enumerate(rows):
                res = client.upload_row(
                    row={"PersonID": row["PersonID"], "AuthorFullName": row["AuthorFullName"],
                         "UT": row["DocumentID"], "OrganizationID": row["OrganizationID"],
                         "match_type": row["match_type"]},
                    is_new_person=row["is_new"],
                    first_name=row["FirstName"], last_name=row["LastName"], delay=0.25,
                )
                ut_log.append({
                    "pid": row["PersonID"], "name": row["AuthorFullName"],
                    "oid": row["OrganizationID"], "ut": row["DocumentID"],
                    "is_new": row["is_new"], "overall": res["overall"],
                    "p_msg":   res["person_step"].message if res["person_step"] else "",
                    "pub_msg": res["pub_step"].message    if res["pub_step"]    else "",
                    "was_dry": dry_run,
                })
                prog.progress((i + 1) / len(rows))
            st.session_state.upload_log[ut]     = ut_log
            st.session_state.ut_upload_done[ut] = True
            # Auto-advance to next unlocked, not-yet-uploaded UT
            ut_order = st.session_state.ut_order
            cur_idx  = st.session_state.ut_index
            for i in range(cur_idx + 1, len(ut_order)):
                nxt = ut_order[i]
                if not st.session_state.ut_locked.get(nxt):
                    st.session_state.ut_index = i
                    break
            st.rerun()
    else:
        # Show results
        n_ok   = sum(1 for e in log if e["overall"] == "ok")
        n_skip = sum(1 for e in log if e["overall"] == "skipped")
        n_err  = sum(1 for e in log if e["overall"] == "error")
        was_dry = log[0].get("was_dry", dry_run) if log else dry_run
        mode    = "🔬 Simulated (dry run)" if was_dry else "✅ Uploaded to MyOrg"
        st.markdown(
            f'<div style="font-size:.82rem;margin:.4rem 0 .6rem;">'
            f'<b>{mode}</b> &nbsp; '
            f'<span style="color:#27ae60;font-weight:600;">✓ {n_ok} ok</span> &nbsp; '
            f'<span style="color:#888;">⏭ {n_skip} skipped</span> &nbsp; '
            f'<span style="color:#e74c3c;">✗ {n_err} errors</span></div>',
            unsafe_allow_html=True,
        )
        for e in log:
            icon = {"ok":"✅","skipped":"⏭","error":"❌"}.get(e["overall"],"⏳")
            nb   = " <b style='color:#9b59b6;font-size:.7rem;'>NEW</b>" if e["is_new"] else ""
            raw_msg = e.get("pub_msg") or e.get("p_msg") or ""
            msg = raw_msg.replace("[DRY RUN] ", "").replace("[DRY RUN]", "")[:100]
            st.markdown(
                f'<div class="upl-result {e["overall"]}">'
                f'<span>{icon}</span>'
                f'<span class="r-pid">{e["pid"]}</span>'
                f'<span class="r-name">{e["name"]}{nb}</span>'
                f'<span class="r-msg">{msg}</span></div>',
                unsafe_allow_html=True,
            )
        if n_err:
            st.error(f"{n_err} error(s) — see Tab 3 for details.")
        if st.button("🔄 Re-upload", key=f"reupload_{_safe_key(ut)}"):
            st.session_state.ut_upload_done.pop(ut, None)
            st.session_state.upload_log.pop(ut, None)
            st.rerun()


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ API Settings")
    _has_cfg_key = bool(load_config("config.json").get("api_key","").strip())
    _key_hint    = "Loaded from config.json" if _has_cfg_key else "Paste X-ApiKey here…"
    key_in = st.text_input("Clarivate API Key", type="password",
                           value=st.session_state.global_api_key,
                           placeholder=_key_hint,
                           key="sidebar_api_key",
                           help="Key is pre-loaded from config.json. Type here to override for this session.")
    if key_in != st.session_state.global_api_key:
        st.session_state.global_api_key = key_in

    dr = st.checkbox("🔬 Dry run (no real calls)",
                     value=st.session_state.global_dry_run, key="sidebar_dry_run")
    if dr != st.session_state.global_dry_run:
        st.session_state.global_dry_run = dr

    ak, dry = st.session_state.global_api_key, st.session_state.global_dry_run
    if dry:
        st.markdown('<div class="api-bar api-dry">🔬 Dry run — simulated calls only</div>',
                    unsafe_allow_html=True)
    elif ak:
        st.markdown('<div class="api-bar api-ok">🟢 Live — real API calls</div>',
                    unsafe_allow_html=True)
        if st.button("🔌 Test connection", key="sb_test"):
            with st.spinner("Testing…"):
                r = MyOrgClient(ak).test_connection()
            (st.success if r.success else st.error)(r.message)
    else:
        st.markdown('<div class="api-bar api-off">⚪ No key — enable dry run or add key</div>',
                    unsafe_allow_html=True)

    st.markdown("---")
    if st.session_state.processed:
        st.markdown("### Publications")
        for i, u in enumerate(st.session_state.ut_order):
            up = st.session_state.ut_upload_done.get(u)
            dn = _ut_is_done(u)
            icon = "🚀" if up else ("✅" if dn else ("⏳" if _ut_needs_attention(u) else "—"))
            if st.button(f"{icon} {u}", key=f"sb_{i}", use_container_width=True):
                st.session_state.ut_index = i
                st.rerun()


# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_load, tab_review, tab_log = st.tabs([
    "📂 1 · Load Files",
    "🔍 2 · Review & Upload",
    "📋 3 · Upload Log",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — LOAD
# ══════════════════════════════════════════════════════════════════════════════
with tab_load:
    st.markdown('<div class="sec-head">Upload files</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    with c1: wos_file = st.file_uploader("WoS Export CSV", type=["csv","txt"], key="wos_up")
    with c2: res_file = st.file_uploader("ResearcherAndDocument.csv", type=["csv"], key="res_up")
    with c3: org_file = st.file_uploader("OrganizationHierarchy.csv", type=["csv"], key="org_up")

    if wos_file and res_file and org_file:
        if st.button("⚙️  Process files", type="primary", use_container_width=True):
            with st.spinner("Parsing and matching…"):
                wos_c = wos_file.read().decode("utf-8-sig")
                res_c = res_file.read().decode("utf-8-sig")
                org_c = org_file.read().decode("utf-8-sig")
                cfg   = load_config("config.json")
                pi, mpid, ep = build_person_index(res_c)
                orgs  = parse_org_hierarchy(org_c)
                recs  = parse_wos_csv(wos_c)
                pairs = extract_muv_author_pairs(recs, cfg)
                res   = batch_process(pairs, pi, orgs, cfg, mpid+1, res_c, ep)
                all_uts = sorted({r.get("UT","") for r in
                                  res["confirmed"]+res["needs_review"]+res["already_uploaded"]
                                  if r.get("UT")})
                ut_order = sorted(all_uts, key=lambda u: (
                    0 if any(r["UT"]==u for r in res["needs_review"]+res["confirmed"]) else 1, u))
                st.session_state.update({
                    "processed": True, "batch_result": res, "person_index": pi,
                    "existing_pairs": ep, "orgs": orgs, "cfg": cfg,
                    "ut_order": ut_order, "ut_index": 0, "max_pid": mpid,
                    "ut_locked": {}, "author_decs": {}, "source_file": wos_file.name,
                    "ut_rows_cache": {}, "ut_upload_done": {}, "upload_log": {},
                })
            st.success(f"✅ {len(recs)} records · {len(pairs)} MUV pairs · {len(ut_order)} UTs")
            st.info("➡️ Go to **Tab 2** to review and upload.")
    else:
        st.info("Upload all three files to begin.")

    if st.session_state.processed:
        res   = st.session_state.batch_result
        au_all = res["already_uploaded"]
        st.markdown('<div class="sec-head">Summary</div>', unsafe_allow_html=True)
        st.markdown(f"""
<div class="metric-grid">
  <div class="metric-card"><div class="num num-blue">{len(st.session_state.ut_order)}</div><div class="lbl">UTs</div></div>
  <div class="metric-card"><div class="num num-green">{len(res['confirmed'])}</div><div class="lbl">Auto-confirmed</div></div>
  <div class="metric-card"><div class="num num-amber">{len(res['needs_review'])}</div><div class="lbl">Needs decision</div></div>
  <div class="metric-card"><div class="num">{len([r for r in au_all if r.get('match_type')!='probable_duplicate'])}</div><div class="lbl">Already in MyOrg</div></div>
  <div class="metric-card"><div class="num num-red">{len([r for r in au_all if r.get('match_type')=='probable_duplicate'])}</div><div class="lbl">Prob. duplicates</div></div>
</div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — REVIEW & UPLOAD
# ══════════════════════════════════════════════════════════════════════════════
with tab_review:
    if not st.session_state.processed:
        st.info("⬅️ Load files in Tab 1 first.")
        st.stop()

    res          = st.session_state.batch_result
    person_index = st.session_state.person_index
    orgs         = st.session_state.orgs
    cfg          = st.session_state.cfg
    ut_order     = st.session_state.ut_order
    org_map, _   = build_org_map(orgs)

    if not ut_order:
        st.success("Nothing to review.")
        st.stop()

    n_done = sum(1 for u in ut_order if _ut_is_done(u))
    n_up   = sum(1 for u in ut_order if st.session_state.ut_upload_done.get(u))
    n_need = sum(1 for u in ut_order if _ut_needs_attention(u))
    pct    = int(100 * n_done / len(ut_order)) if ut_order else 100

    st.markdown(f"""
<div style="display:flex;justify-content:space-between;font-size:.8rem;color:#5a7080;">
  <span>Progress</span>
  <span><b>{n_done}</b>/{len(ut_order)} confirmed &nbsp;·&nbsp;
        <b>{n_up}</b> uploaded &nbsp;·&nbsp;
        <b>{n_need}</b> need decisions</span>
</div>
<div class="prog-bar-wrap"><div class="prog-bar-fill" style="width:{pct}%"></div></div>
""", unsafe_allow_html=True)

    idx = max(0, min(st.session_state.ut_index, len(ut_order)-1))

    nav_l, nav_c, nav_r = st.columns([1, 4, 1])
    with nav_l:
        if st.button("◀ Prev", use_container_width=True, disabled=(idx==0)):
            st.session_state.ut_index = max(0, idx-1); st.rerun()
    with nav_r:
        if st.button("Next ▶", use_container_width=True, disabled=(idx>=len(ut_order)-1)):
            st.session_state.ut_index = min(len(ut_order)-1, idx+1); st.rerun()
    with nav_c:
        def _icon(u):
            if st.session_state.ut_upload_done.get(u): return "🚀"
            if _ut_is_done(u): return "✅"
            if _ut_needs_attention(u): return "⏳"
            return "—"
        ut_disp = [f"{_icon(u)}  {u}" for u in ut_order]
        jump = st.selectbox("Jump", ut_disp, index=idx, label_visibility="collapsed")
        ji = ut_disp.index(jump)
        if ji != idx:
            st.session_state.ut_index = ji; st.rerun()

    ut = ut_order[idx]
    status = _ut_status(ut)

    auto_rows = _ut_auto_confirmed(ut)
    rev_rows  = _ut_needs_attention(ut)
    dup_rows  = _ut_already_uploaded(ut)

    # UT header badge
    _cb = ""
    if status == "locked":
        _cb = ("<span style='color:#1a9dc8;font-weight:600;margin-left:.8rem;'>🚀 UPLOADED</span>"
               if st.session_state.ut_upload_done.get(ut) else
               "<span style='color:#27ae60;font-weight:600;margin-left:.8rem;'>✅ LOCKED</span>")
    elif status == "skip":
        _cb = "<span style='color:#888;margin-left:.8rem;'>⏭ auto-skipped</span>"
    elif not rev_rows:
        _cb = "<span style='color:#27ae60;margin-left:.8rem;'>✅ auto-done</span>"

    st.markdown(f"""
<div class="ut-card">
  <div class="ut-id">{ut}</div>
  <div class="ut-meta">
    <b>{len(auto_rows)}</b> auto-confirmed &nbsp;·&nbsp;
    <b>{len(rev_rows)}</b> need decision &nbsp;·&nbsp;
    <b>{len(dup_rows)}</b> already in MyOrg {_cb}
  </div>
</div>""", unsafe_allow_html=True)

    if dup_rows:
        with st.expander(f"⏭  {len(dup_rows)} already in MyOrg", expanded=False):
            for r in dup_rows:
                mt = r.get("match_type","")
                bc = "badge-dup" if mt=="probable_duplicate" else "badge-skip"
                bl = "PROB. DUP" if mt=="probable_duplicate" else "ALREADY IN"
                sc = f" ({r['match_score']:.2f})" if mt=="probable_duplicate" else ""
                st.markdown(f'<span class="badge {bc}">{bl}{sc}</span> '
                            f'<b>{r.get("author_full",r.get("AuthorFullName",""))}</b>'
                            f' → {r.get("AuthorFullName","")} [{r.get("PersonID","")}]',
                            unsafe_allow_html=True)

    if auto_rows:
        with st.expander(f"✅  {len(auto_rows)} auto-confirmed", expanded=False):
            for r in auto_rows:
                st.markdown(f'<span class="badge badge-exact">EXACT</span> '
                            f'<b>{r.get("author_full",r.get("AuthorFullName",""))}</b>'
                            f' → {r.get("AuthorFullName","")} [{r.get("PersonID","")}]'
                            f' <span class="chip">{r.get("OrganizationID","")}</span>',
                            unsafe_allow_html=True)

    # ── Locked view ───────────────────────────────────────────────────────────
    if status == "locked":
        st.markdown('<div class="locked-ut">🔒 Publication confirmed</div>', unsafe_allow_html=True)
        _show_ut_upload_section(ut)
        if st.button("🔓 Unlock to re-edit", key=f"unlock_{ut}"):
            st.session_state.ut_locked[ut] = False
            st.session_state.ut_index = idx
            for d in ("ut_rows_cache","ut_upload_done","upload_log"):
                st.session_state[d].pop(ut, None)
            st.rerun()

    elif status == "skip":
        st.success("All authors already in MyOrg — nothing to do.")

    else:
        # ── Author cards ──────────────────────────────────────────────────────
        if rev_rows:
            st.markdown('<div class="sec-head">Authors needing a decision</div>',
                        unsafe_allow_html=True)

        for r in rev_rows:
            raw  = r.get("AuthorFullName", r.get("author_full",""))
            norm = normalize_name(raw)
            mt   = r.get("match_type","new")
            dec  = _get_dec(norm, ut)

            if not dec:
                dec = {"decided": False, "action": "approve",
                       "resolved_pid": r.get("suggested_pid",""),
                       "resolved_name": r.get("suggested_name", raw),
                       "org_ids": r.get("suggested_org_ids") or [""],
                       "match_type": mt, "_search": ""}
                _set_dec(norm, ut, dec)

            decided = dec.get("decided", False)
            bm = {"initial_expansion":("badge-initial","INITIAL"),
                  "fuzzy":("badge-fuzzy","FUZZY"),"new":("badge-new","NEW")}
            bcls, blbl = bm.get(mt, ("badge-new", mt.upper()))
            chips = " ".join(f'<span class="chip">{a}</span>'
                             for a in r.get("muv_affils",[r.get("RawAffil","")])[:3])
            ccls = ("author-row locked"   if decided and dec.get("action")=="approve" else
                    "author-row rejected" if decided and dec.get("action")=="reject"  else
                    "author-row")
            st.markdown(f'<div class="{ccls}"><span class="badge {bcls}">{blbl}</span>'
                        f'<b>{raw}</b> <span style="font-size:.75rem;color:#7a8fa0;">{chips}</span></div>',
                        unsafe_allow_html=True)

            with st.container():
                left, right = st.columns([3, 2])
                with left:
                    cands = r.get("candidates", [])
                    if mt in ("fuzzy","initial_expansion") and cands:
                        cl = [f"[{p['PersonID']}] {p['AuthorFullName']}  ({s:.2f})" for s,p,_ in cands]
                        cl.append("➕ Create as NEW PERSON")
                        sc = dec.get("_cand_choice", cl[0])
                        di = cl.index(sc) if sc in cl else 0
                        ch = st.selectbox(f"Identity for **{raw}**", cl, index=di,
                                          key=_safe_key("cand",norm,ut), disabled=decided)
                        dec["_cand_choice"] = ch
                        if not dec.get("_override_pid"):
                            if "NEW PERSON" in ch:
                                dec.update({"resolved_pid":r.get("suggested_pid",""),
                                            "resolved_name":raw,"match_type":"new","org_ids":[""]})
                            else:
                                ic = cl.index(ch); _, cp, _ = cands[ic]
                                dec.update({"resolved_pid":cp["PersonID"],
                                            "resolved_name":cp["AuthorFullName"],
                                            "match_type":"resolved",
                                            "org_ids":cp.get("OrganizationIDs") or
                                                      ([cp["OrganizationID"]] if cp.get("OrganizationID") else [""])})

                        ovr_a = bool(dec.get("_override_pid"))
                        ovr_l = (f"✏️ Override: {dec.get('resolved_name','')} [{dec.get('resolved_pid','')}]"
                                 if ovr_a else "🔍 Not right? Search full list")
                        with st.expander(ovr_l, expanded=ovr_a):
                            if ovr_a and not decided:
                                if st.button("✖ Clear override", key=_safe_key("clr_ovr",norm,ut)):
                                    for k in ("_override_pid","_override_query","_override_choice"):
                                        dec.pop(k,None)
                                    _set_dec(norm,ut,dec); st.rerun()
                            ovr_qk = _safe_key("ovr_q",norm,ut)
                            ovr_pk = _safe_key("ovr_pick",norm,ut)
                            pq = dec.get("_override_query","")
                            ovq = st.text_input("Search",value=pq,key=ovr_qk,
                                                disabled=decided,placeholder="Type a name…")
                            if ovq != pq:
                                dec["_override_query"]=ovq; dec.pop("_override_choice",None)
                                st.session_state.pop(ovr_pk,None)
                            dec["_override_query"]=ovq
                            if ovq:
                                oh = search_persons(ovq, person_index)
                                if oh:
                                    st.caption(f"🔍 {len(oh)} match{'es' if len(oh)!=1 else ''}")
                                    NO = f"➕ Create as NEW PERSON  (ID {r.get('suggested_pid','')})"
                                    oo = [NO]+[f"[{hp['PersonID']}] {hp['AuthorFullName']}  ·  {int(hs*100)}%" for hs,hp in oh]
                                    om = {f"[{hp['PersonID']}] {hp['AuthorFullName']}  ·  {int(hs*100)}%":hp for hs,hp in oh}
                                    so = dec.get("_override_choice",NO)
                                    od = oo.index(so) if so in oo else 0
                                    oc = st.selectbox("Select",oo,index=od,key=ovr_pk,disabled=decided)
                                    dec["_override_choice"]=oc
                                    if oc != NO:
                                        op=om[oc]; no=op.get("OrganizationIDs") or ([op["OrganizationID"]] if op.get("OrganizationID") else [])
                                        dec.update({"resolved_pid":op["PersonID"],"resolved_name":op["AuthorFullName"],
                                                    "match_type":"resolved","_override_pid":op["PersonID"],"org_ids":no or [""]})
                                        ok2=_safe_key("orgs",norm,ut)
                                        vl=[org_label(o,org_map) for o in no if o]; vl=[l for l in vl if l in org_map]
                                        if vl: st.session_state[ok2]=vl
                                    else:
                                        dec.update({"resolved_pid":r.get("suggested_pid",""),"resolved_name":raw,
                                                    "match_type":"new","org_ids":[""]}); dec.pop("_override_pid",None)
                                else:
                                    st.caption("No matches found.")
                    else:
                        sk = _safe_key("search",norm,ut); pk = _safe_key("search_pick",norm,ut)
                        NL = f"➕ Create as NEW PERSON  (ID {r.get('suggested_pid','')})"
                        pq2 = dec.get("_search", raw)
                        sq = st.text_input(f"Search existing for **{raw}**", value=pq2, key=sk,
                                           disabled=decided, placeholder="Type a name and press Enter…")
                        if sq != pq2:
                            dec["_search"]=sq; dec["_search_choice"]=NL; st.session_state.pop(pk,None)
                        hits = search_persons(sq, person_index)
                        opts = [NL]+[f"[{hp['PersonID']}] {hp['AuthorFullName']}  ·  {int(hs*100)}%" for hs,hp in hits]
                        hm   = {f"[{hp['PersonID']}] {hp['AuthorFullName']}  ·  {int(hs*100)}%":hp for hs,hp in hits}
                        if sq and sq != raw:
                            st.caption(f"🔍 {len(hits)} match{'es' if len(hits)!=1 else ''}" if hits else "No matches")
                        ss = dec.get("_search_choice",NL); sd = opts.index(ss) if ss in opts else 0
                        sch = st.selectbox(f"Identity for **{raw}**", opts, index=sd, key=pk, disabled=decided)
                        dec["_search_choice"]=sch; dec["_search"]=sq
                        if not dec.get("_override_pid"):
                            if sch==NL:
                                dec.update({"resolved_pid":r.get("suggested_pid",""),"resolved_name":raw,
                                            "match_type":"new","org_ids":[""]})
                            else:
                                hp=hm[sch]
                                dec.update({"resolved_pid":hp["PersonID"],"resolved_name":hp["AuthorFullName"],
                                            "match_type":"resolved",
                                            "org_ids":hp.get("OrganizationIDs") or ([hp["OrganizationID"]] if hp.get("OrganizationID") else [""])})

                    if dec.get("resolved_pid") and dec.get("resolved_name"):
                        if dec["match_type"]=="new":
                            st.caption(f"📋 New person · ID {dec['resolved_pid']}")
                        else:
                            st.caption(f"✔ → {dec['resolved_name']} (ID {dec['resolved_pid']})")
                            if (dec["resolved_pid"], ut) in st.session_state.existing_pairs:
                                st.warning("⚠️ Already in MyOrg — skipped on upload.")

                with right:
                    ok3 = _safe_key("orgs",norm,ut)
                    dl  = [org_label(o,org_map) for o in dec.get("org_ids",[""]) if o and org_label(o,org_map) in org_map]
                    sel = st.multiselect("Organisation(s)", list(org_map.keys()),
                                         default=dl if ok3 not in st.session_state else None,
                                         key=ok3, disabled=decided)
                    dec["org_ids"] = [org_map[l] for l in sel] or [""]

                    st.markdown("")
                    ac, rc = st.columns(2)
                    with ac:
                        if st.button("✅ Approve" if not decided or dec.get("action")=="reject" else "✅ Approved",
                                     key=_safe_key("approve",norm,ut), use_container_width=True,
                                     type="primary" if not decided else "secondary",
                                     disabled=(decided and dec.get("action")=="approve")):
                            dec["decided"]=True; dec["action"]="approve"; _set_dec(norm,ut,dec); st.rerun()
                    with rc:
                        if st.button("❌ Reject" if not decided or dec.get("action")=="approve" else "❌ Rejected",
                                     key=_safe_key("reject",norm,ut), use_container_width=True,
                                     disabled=(decided and dec.get("action")=="reject")):
                            dec["decided"]=True; dec["action"]="reject"; _set_dec(norm,ut,dec); st.rerun()
                    if decided:
                        if st.button("✏️ Undo", key=_safe_key("undo",norm,ut), use_container_width=True):
                            dec["decided"]=False; _set_dec(norm,ut,dec); st.rerun()

            _set_dec(norm, ut, dec)
            st.markdown("<hr style='margin:.5rem 0;border-color:#e0e8ef;'>", unsafe_allow_html=True)

        # ── Confirm & Upload button ────────────────────────────────────────────
        st.markdown("")
        can_lock = _all_authors_decided(ut) or not rev_rows
        if st.button("🔒 Confirm & Upload Publication",
                     type="primary", use_container_width=True,
                     disabled=not can_lock,
                     help="" if can_lock else "Decide all authors first.",
                     key=f"lock_{ut}"):
            st.session_state.ut_locked[ut] = True
            _build_ut_rows(ut)
            st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — UPLOAD LOG
# ══════════════════════════════════════════════════════════════════════════════
with tab_log:
    if not st.session_state.processed:
        st.info("⬅️ Load files in Tab 1 first.")
        st.stop()

    ulog  = st.session_state.upload_log
    dry   = st.session_state.global_dry_run
    t_row = sum(len(v) for v in ulog.values())
    t_ok  = sum(1 for v in ulog.values() for e in v if e["overall"]=="ok")
    t_sk  = sum(1 for v in ulog.values() for e in v if e["overall"]=="skipped")
    t_er  = sum(1 for v in ulog.values() for e in v if e["overall"]=="error")

    st.markdown(f"""
<div class="metric-grid">
  <div class="metric-card"><div class="num num-blue">{len(ulog)}</div><div class="lbl">UTs processed</div></div>
  <div class="metric-card"><div class="num num-green">{t_ok}</div><div class="lbl">{'Simulated' if dry else 'Uploaded'}</div></div>
  <div class="metric-card"><div class="num">{t_sk}</div><div class="lbl">Skipped</div></div>
  <div class="metric-card"><div class="num num-red">{t_er}</div><div class="lbl">Errors</div></div>
  <div class="metric-card"><div class="num">{t_row}</div><div class="lbl">Total rows</div></div>
</div>""", unsafe_allow_html=True)

    if not ulog:
        st.info("No uploads yet — confirm publications in Tab 2.")
        st.stop()

    all_e = [{"UT":ut,**e} for ut,es in ulog.items() for e in es]
    ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
    st.download_button("⬇️ Download full log CSV",
                       pd.DataFrame(all_e).to_csv(index=False).encode("utf-8"),
                       f"upload_log_{ts}.csv", "text/csv")

    st.markdown('<div class="sec-head">Per-publication</div>', unsafe_allow_html=True)
    for ut, entries in ulog.items():
        nok=sum(1 for e in entries if e["overall"]=="ok")
        nsk=sum(1 for e in entries if e["overall"]=="skipped")
        ner=sum(1 for e in entries if e["overall"]=="error")
        with st.expander(f"{'⚠' if ner else '🚀'} {ut}  —  ✓{nok} ⏭{nsk} ✗{ner}",
                         expanded=(ner>0)):
            for e in entries:
                icon={"ok":"✅","skipped":"⏭","error":"❌"}.get(e["overall"],"⏳")
                nb=" <b style='color:#9b59b6;font-size:.7rem;'>NEW</b>" if e["is_new"] else ""
                raw_m = e.get("pub_msg") or e.get("p_msg") or ""
                clean = raw_m.replace("[DRY RUN] ","").replace("[DRY RUN]","")[:120]
                msg   = f"(dry run) {clean}" if e.get("was_dry") else clean
                st.markdown(
                    f'<div class="upl-result {e["overall"]}">'
                    f'<span>{icon}</span><span class="r-pid">{e["pid"]}</span>'
                    f'<span class="r-name">{e["name"]}{nb}</span>'
                    f'<span class="r-msg">{msg}</span></div>',
                    unsafe_allow_html=True)
