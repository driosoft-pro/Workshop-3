"""bootstrap_dashboards.py — create the happiness KPI assets in Superset.

Idempotent: re-running only creates what is missing. Uses the Superset REST API
(session login + CSRF, then JSON endpoints). No third-party dependencies.

Usage:
    python dashboards/superset/bootstrap_dashboards.py            # all steps
    python dashboards/superset/bootstrap_dashboards.py --verify  # query checks only

What it creates:
  * database connection  "happiness"        (PostgreSQL, host `postgres`)
  * datasets             vw_kpi_* / vw_pipeline_status (+1 virtual, pre-sorted)
  * charts               2 big numbers, 4 tables, 1 bar chart, 1 pie chart
  * dashboard            "Happiness KPIs"   (all charts in a 24-col grid)
"""
from __future__ import annotations

import argparse
import http.cookiejar
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

BASE = "http://localhost:8088"
DB_NAME = "happiness"
SQLALCHEMY_URI = "postgresql+psycopg2://happiness:happiness@postgres:5432/happiness"
DASHBOARD_TITLE = "Happiness KPIs"

# The dashboard<->chart link lives in `dashboard_slices`, which the REST API
# does not expose: the position_json we PUT is layout only. We sync it with a
# tiny ORM snippet executed inside the running Superset container.
LINK_SNIPPET = """
from superset.app import create_app
app = create_app()
with app.app_context():
    from superset import db
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice
    dash = db.session.query(Dashboard).filter_by(dashboard_title=%(title)r).one()
    slices = db.session.query(Slice).filter(Slice.slice_name.in_(%(names)r)).all()
    dash.slices = slices
    db.session.commit()
    print("linked %%d charts" %% len(slices))
"""

# (table_name, sql or None for a physical table on a view)
DATASETS = [
    ("vw_kpi_avg_error", None),
    ("vw_kpi_pred_vs_actual", None),
    ("vw_kpi_trend_by_year", None),
    ("vw_pipeline_status", None),
    ("vw_kpi_by_model", None),
    ("top_countries_by_mae",
     "SELECT country_name, region, n_predictions, mae "
     "FROM vw_kpi_predictions_by_country "
     "ORDER BY mae DESC, n_predictions DESC LIMIT 15"),
]


def agg(col: str, op: str = "AVG") -> dict:
    """Ad-hoc aggregate metric over a column (Superset 4.x metric dict)."""
    return {"expressionType": "SIMPLE", "clause": "AGGREGATE",
            "column": {"column_name": col}, "aggregate": op,
            "label": f"{op}({col})",
            "optionName": f"metric_{col}_{op}".lower()}


# (chart_name, dataset, viz_type, params)
CHARTS = [
    ("Overall MAE", "vw_kpi_avg_error", "big_number",
     {"metric": agg("mae"), "time_range": "No filter", "y_axis_format": ".3f"}),
    ("Predictions scored", "vw_kpi_pred_vs_actual", "big_number",
     {"metric": agg("actual_score", "COUNT"), "time_range": "No filter",
      "y_axis_format": ",.0f"}),
    ("KPI summary (MAE / RMSE / bias)", "vw_kpi_avg_error", "table",
     {"all_columns": ["mae", "rmse", "bias", "n_predictions"],
      "row_limit": 100, "include_search": False, "show_cell_bars": False}),
    ("Top 15 countries by MAE", "top_countries_by_mae", "table",
     {"all_columns": ["country_name", "region", "n_predictions", "mae"],
      "row_limit": 100, "include_search": True, "show_cell_bars": True}),
    ("Pipeline status (contract validation)", "vw_pipeline_status", "table",
     {"all_columns": ["processing_status", "n_events"],
      "row_limit": 100, "show_cell_bars": True}),
    ("MAE by model version", "vw_kpi_by_model", "table",
     {"all_columns": ["model_version", "algorithm", "n_predictions", "mae"],
      "row_limit": 100, "show_cell_bars": True}),
    ("Avg actual vs predicted by year", "vw_kpi_trend_by_year", "dist_bar",
     {"metrics": [agg("avg_actual"), agg("avg_predicted"), agg("mae")],
      "groupby": ["observation_year"], "row_limit": 100,
      "time_range": "No filter", "show_legend": True, "bar_stacked": False,
      "x_ticks_layout": "auto", "y_axis_format": "SMART_NUMBER"}),
    ("Events by processing status", "vw_pipeline_status", "pie",
     {"metrics": [agg("n_events", "SUM")], "groupby": ["processing_status"],
      "row_limit": 100, "show_labels": True, "labels_outside": True,
      "label_type": "key_percent", "legend_type": "scroll"}),
]

# chart_name -> (x, y, width, height) on a 24-column grid
LAYOUT = {
    "Overall MAE": (0, 0, 6, 4),
    "Predictions scored": (6, 0, 6, 4),
    "Events by processing status": (12, 0, 12, 8),
    "Avg actual vs predicted by year": (0, 8, 12, 8),
    "KPI summary (MAE / RMSE / bias)": (12, 8, 12, 8),
    "Top 15 countries by MAE": (0, 16, 8, 8),
    "MAE by model version": (8, 16, 8, 8),
    "Pipeline status (contract validation)": (16, 16, 8, 8),
}


class Superset:
    def __init__(self) -> None:
        self.cj = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cj))
        self.csrf = ""

    def _open(self, req):
        try:
            with self.opener.open(req, timeout=120) as resp:
                return resp.getcode(), resp.read().decode()
        except urllib.error.HTTPError as exc:  # noqa: BLE001
            return exc.code, exc.read().decode()

    def get(self, path: str):
        code, body = self._open(urllib.request.Request(BASE + path))
        return code, body

    def post(self, path: str, payload, method: str = "POST"):
        req = urllib.request.Request(
            BASE + path, data=json.dumps(payload).encode(), method=method,
            headers={"Content-Type": "application/json",
                     "X-CSRFToken": self.csrf, "Referer": BASE + "/"})
        return self._open(req)

    def login(self) -> None:
        code, html = self.get("/login/")
        if code != 200:
            sys.exit(f"login page -> HTTP {code}")
        m = re.search(r'name="csrf_token"[^>]*value="([^"]+)"', html)
        if not m:
            sys.exit("csrf token not found on /login/")
        form = urllib.parse.urlencode(
            {"username": "admin", "password": "admin", "csrf_token": m.group(1)}
        ).encode()
        req = urllib.request.Request(
            BASE + "/login/", data=form,
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        self._open(req)
        code, body = self.get("/api/v1/security/csrf_token/")
        if code != 200:
            sys.exit(f"session login failed (csrf_token -> HTTP {code})")
        self.csrf = json.loads(body)["result"]

    # ------------------------------------------------------------------ API
    def list_(self, endpoint: str) -> list[dict]:
        code, body = self.get(f"/api/v1/{endpoint}/?q=(page_size:1000)")
        if code != 200:
            raise RuntimeError(f"GET {endpoint} -> HTTP {code}: {body[:200]}")
        return json.loads(body).get("result") or []

    def create(self, endpoint: str, payload: dict, label: str) -> dict:
        code, body = self.post(f"/api/v1/{endpoint}/", payload)
        if code not in (200, 201):
            raise RuntimeError(f"POST {endpoint} ({label}) -> HTTP {code}: "
                               f"{body[:400]}")
        data = json.loads(body)
        obj = data.get("result") or data.get("data") or data
        if "id" not in obj and "id" in data:
            obj = {**obj, "id": data["id"]}
        return obj


def build_query_context(ds_id: int, viz: str, params: dict) -> dict:
    """Query context matching the chart params (what Explore would save)."""
    metrics = list(params.get("metrics") or [])
    if params.get("metric"):
        metrics = [params["metric"]]
    query = {
        "time_range": params.get("time_range", "No filter"),
        "granularity_sqla": None, "time_offsets": [],
        "groupby": params.get("groupby", []),
        "metrics": metrics,
        "columns": params.get("all_columns", []),
        "orderby": [], "filters": [], "having": "", "where": "",
        "row_limit": params.get("row_limit", 1000), "order_desc": True,
    }
    return {"datasource": {"id": ds_id, "type": "table"}, "force": False,
            "queries": [query], "result_type": "full",
            "result_format": "json"}


def position_json(chart_ids: dict[str, int]) -> str:
    """Canonical Superset layout: ROOT_ID -> GRID_ID -> ROW-* -> CHART-*.

    The ids MUST be the literals from ``src/dashboard/util/constants.ts``
    (``ROOT_ID``, ``GRID_ID``, ``HEADER_ID``): the frontend does
    ``layout[DASHBOARD_ROOT_ID]`` on the parsed position_json, so a wrong root
    key makes ``findFirstParentContainerId`` throw and the dashboard never
    finishes loading. ``HEADER_ID`` is injected by ``hydrate.js`` itself.
    """
    root, grid = "ROOT_ID", "GRID_ID"
    positions: dict[str, dict] = {
        "DASHBOARD_VERSION_KEY": "v2",
        root: {"type": "ROOT", "id": root, "children": [grid]},
    }
    grid_children: list[str] = []
    for row_y in sorted({y for _, y, _, _ in LAYOUT.values()}):
        row_id = f"ROW-{row_y}"
        row_children: list[str] = []
        for name, (x, y, w, h) in LAYOUT.items():
            if y != row_y:
                continue
            chart_id = f"CHART-{chart_ids[name]}"
            row_children.append(chart_id)
            positions[chart_id] = {
                "type": "CHART", "id": chart_id, "children": [],
                "parents": [root, grid, row_id],
                "meta": {"chartId": chart_ids[name], "sliceName": name,
                         "width": w, "height": h},
            }
        positions[row_id] = {
            "type": "ROW", "id": row_id, "children": row_children,
            "parents": [root, grid],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        grid_children.append(row_id)
    positions[grid] = {"type": "GRID", "id": grid, "children": grid_children,
                       "parents": [root]}
    return json.dumps(positions)


def link_dashboard_charts(names: list[str]) -> None:
    """Sync dashboard_slices by running a snippet inside the Superset container."""
    import shutil
    import subprocess

    cli = next((c for c in ("podman", "docker") if shutil.which(c)), None)
    if not cli:
        print("[warn] podman/docker not found: skip dashboard<->chart link")
        return
    ps = subprocess.run([cli, "ps", "--format", "{{.Names}}"],
                        capture_output=True, text=True)
    running = [n for n in ps.stdout.split()
               if "superset" in n and "init" not in n]
    if not running:
        print("[warn] no running superset container: skip dashboard link")
        return
    snippet = LINK_SNIPPET % {"title": DASHBOARD_TITLE, "names": names}
    proc = subprocess.run([cli, "exec", running[0], "python", "-c", snippet],
                          capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"dashboard link failed: {proc.stderr[-400:]}")
    print(f"[ok] {proc.stdout.strip()} ({cli} exec {running[0]})")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify", action="store_true",
                        help="only run the chart query checks")
    args = parser.parse_args()

    s = Superset()
    s.login()
    print("[ok] logged in")

    if not args.verify:
        # 1. database connection -------------------------------------------
        dbs = [d for d in s.list_("database") if d["database_name"] == DB_NAME]
        if dbs:
            print(f"[skip] database '{DB_NAME}' exists (id={dbs[0]['id']})")
        else:
            db = s.create("database", {
                "database_name": DB_NAME, "sqlalchemy_uri": SQLALCHEMY_URI,
                "expose_in_sqllab": True}, DB_NAME)
            print(f"[ok] database '{DB_NAME}' created (id={db['id']})")
        db_id = [d for d in s.list_("database")
                 if d["database_name"] == DB_NAME][0]["id"]

        # 2. datasets -------------------------------------------------------
        existing_ds = {d["table_name"]: d["id"]
                       for d in s.list_("dataset")}
        for table_name, sql in DATASETS:
            if table_name in existing_ds:
                print(f"[skip] dataset {table_name}")
                continue
            payload = {"database": db_id, "schema": "public",
                       "table_name": table_name}
            if sql:
                payload.update({"schema": None, "sql": sql})
            ds = s.create("dataset", payload, table_name)
            existing_ds[table_name] = ds["id"]
            print(f"[ok] dataset {table_name} (id={ds['id']})")

        # 3. charts ---------------------------------------------------------
        existing_ch = {c["slice_name"]: c for c in s.list_("chart")}
        for name, table, viz, params in CHARTS:
            payload = {
                "slice_name": name, "viz_type": viz,
                "datasource_id": existing_ds[table],
                "datasource_type": "table",
                "params": json.dumps(params),
                "query_context": json.dumps(
                    build_query_context(existing_ds[table], viz, params)),
            }
            if name in existing_ch:
                chart_id = existing_ch[name]["id"]
                code, body = s.post(f"/api/v1/chart/{chart_id}", payload,
                                    method="PUT")
                if code != 200:
                    raise RuntimeError(f"PUT chart ({name}) -> HTTP {code}: "
                                       f"{body[:300]}")
                print(f"[ok] chart updated {name} (id={chart_id})")
            else:
                chart = s.create("chart", payload, name)
                print(f"[ok] chart {name} (id={chart['id']})")

        # 4. dashboard ------------------------------------------------------
        dashes = [d for d in s.list_("dashboard")
                  if d["dashboard_title"] == DASHBOARD_TITLE]
        payload = {
            "dashboard_title": DASHBOARD_TITLE,
            "published": True,
            "position_json": position_json(
                {n: c["id"] for n, c in existing_ch.items()}),
            "json_metadata": json.dumps({
                "refresh_frequency": 0, "expanded_slices": {},
                "label_colors": {}, "default_filters": "{}"}),
        }
        if dashes:
            code, body = s.post(f"/api/v1/dashboard/{dashes[0]['id']}",
                                payload, method="PUT")
            if code != 200:
                raise RuntimeError(f"PUT dashboard -> HTTP {code}: {body[:400]}")
            print(f"[ok] dashboard updated (HTTP {code})")
        else:
            dash = s.create("dashboard", payload, DASHBOARD_TITLE)
            print(f"[ok] dashboard '{DASHBOARD_TITLE}' (id={dash['id']})")

        link_dashboard_charts([name for name, *_ in CHARTS])

    # 5. verify: run every chart's query through the API --------------------
    print("\n[verify] chart queries")
    failed = 0
    charts_by_name = {c["slice_name"]: c for c in s.list_("chart")}
    for name, table, viz, params in CHARTS:
        chart = charts_by_name.get(name)
        if not chart:
            print(f"[fail] {name}: chart missing")
            failed += 1
            continue
        qc = build_query_context(chart["datasource_id"], viz, params)
        code, body = s.post("/api/v1/chart/data", qc)
        if code != 200:
            print(f"[fail] {name}: HTTP {code} {body[:300]}")
            failed += 1
            continue
        result = json.loads(body)
        rows = (result.get("result") or [{}])[0].get("data", [])
        print(f"[ok] {name}: {len(rows)} row(s)")

    # 6. dashboard is linked to every chart ---------------------------------
    dashes = [d for d in s.list_("dashboard")
              if d["dashboard_title"] == DASHBOARD_TITLE]
    if not dashes:
        print(f"[fail] dashboard '{DASHBOARD_TITLE}' missing")
        failed += 1
    else:
        code, body = s.get(f"/api/v1/dashboard/{dashes[0]['id']}/charts")
        linked = json.loads(body).get("result") or [] if code == 200 else []
        if len(linked) == len(CHARTS):
            print(f"[ok] dashboard '{DASHBOARD_TITLE}': "
                  f"{len(linked)}/{len(CHARTS)} charts linked")
        else:
            print(f"[fail] dashboard charts: {len(linked)}/{len(CHARTS)} — "
                  f"run without --verify to relink")
            failed += 1

    print(f"\n{'FAILED: ' + str(failed) if failed else 'all chart queries OK'}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
