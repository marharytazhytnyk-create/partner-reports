#!/usr/bin/env python3
"""
Auto-generating weekly report for IZI BURGER Kharkiv.
Fetches data from Databricks fact_provider_weekly and generates HTML.
Designed to run every Monday at 13:30 Kyiv time via GitHub Actions.
"""

import json
import csv
import base64
import datetime
import os
import sys
import urllib.request
import urllib.error
import tempfile

# ─── CONFIGURATION ───────────────────────────────────────────────────────────

DATABRICKS_HOST = os.environ.get('DATABRICKS_HOST', 'https://bolt-incentives.cloud.databricks.com')
DATABRICKS_TOKEN = os.environ.get('DATABRICKS_TOKEN', '')

CLUSTER_ID = os.environ.get('DATABRICKS_CLUSTER_ID', '0221-081903-9ag4bh69')

# IZI BURGER Kharkiv provider IDs (from delivery_provider_provider table)
IZI_PROVIDER_IDS = [139923, 169006, 184043]
PROVIDERS = {
    '139923': {'name': 'IZI BURGER Стадіонний проїзд', 'address': 'Стадіонний проїзд 3, Харків'},
    '169006': {'name': 'IZI Burger пр. Ювілейний', 'address': 'Ювілейний проспект 56, Харків'},
    '184043': {'name': 'IZI BURGER Григорівське шосе', 'address': 'Григорівське шосе 32, Харків'},
}
PROVIDERS_ORDER = ['139923', '169006', '184043']

OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Щотижневий звіт Izi Burger.html')


# ─── DATABRICKS HELPERS ──────────────────────────────────────────────────────

def db_request(endpoint, method='GET', data=None):
    """Make a Databricks REST API request."""
    import re as _re
    url = f"{DATABRICKS_HOST}{endpoint}"
    headers = {
        'Authorization': f'Bearer {DATABRICKS_TOKEN}',
        'Content-Type': 'application/json',
    }
    body = json.dumps(data).encode('utf-8') if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            # Strip ANSI escape codes that Databricks embeds in error messages
            clean = _re.sub(rb'\x1b\[[0-9;]*[mGKHF]', b'', raw)
            return json.loads(clean.decode('utf-8', errors='replace'))
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code}: {e.read().decode()[:500]}")
        raise


def run_spark_command(code: str) -> str:
    """Execute Python code on Databricks cluster and return output."""
    ctx = db_request('/api/1.2/contexts/create', 'POST', {'clusterId': CLUSTER_ID, 'language': 'python'})
    ctx_id = ctx['id']

    cmd = db_request('/api/1.2/commands/execute', 'POST', {
        'clusterId': CLUSTER_ID,
        'contextId': ctx_id,
        'language': 'python',
        'command': code
    })
    cmd_id = cmd['id']

    import time
    for _ in range(120):
        time.sleep(10)
        status = db_request(
            f'/api/1.2/commands/status?clusterId={CLUSTER_ID}&contextId={ctx_id}&commandId={cmd_id}'
        )
        s = status.get('status', '')
        if s == 'Finished':
            results = status.get('results', {})
            if results.get('resultType') == 'text':
                return results.get('data', '')
            elif results.get('resultType') == 'error':
                raise RuntimeError(f"Spark error: {results.get('summary', '')[:500]}")
        elif s in ('Error', 'Cancelled'):
            raise RuntimeError(f"Command {s}")

    raise TimeoutError("Command timed out after 20 minutes")


def fetch_data():
    """Fetch 8 weeks of data from Databricks and return structured dict."""
    today = datetime.date.today()
    days_since_monday = today.weekday()
    last_monday = today - datetime.timedelta(days=days_since_monday)
    start_date = last_monday - datetime.timedelta(weeks=8)

    week_starts = [start_date + datetime.timedelta(weeks=i) for i in range(8)]

    print(f"Fetching data for weeks: {week_starts[0]} to {week_starts[-1]}")

    pids_str   = ', '.join(str(p) for p in IZI_PROVIDER_IDS)
    start_str  = week_starts[0].strftime('%Y-%m-%d')
    end_str    = (week_starts[-1] + datetime.timedelta(days=6)).strftime('%Y-%m-%d')

    spark_code = f"""
import pyspark.sql.functions as F
import json

izi_prov_ids = [{pids_str}]
df = spark.read.format('delta').load('s3://bolt-delta-lake/ng_delivery/tables/fact_provider_weekly')
izi_data = df.filter(
    F.col('provider_id').isin(izi_prov_ids) &
    (F.col('city_id') == 491) &
    (F.col('metric_timestamp_local').cast('string').substr(1,10) >= '{start_str}') &
    (F.col('metric_timestamp_local').cast('string').substr(1,10) <= '{end_str}')
).select(
    'provider_id', 'city_id', 'metric_timestamp_local',
    'delivered_orders_count', 'placed_orders_count',
    'failed_order_rate_value',
    'total_provider_price_before_discounts', 'total_provider_price_after_discounts',
    'provider_acceptance_rate_value', 'courier_delivery_completion_rate_value',
    'bad_order_rate_value', 'bad_provider_rating_rate_value', 'courier_acceptance_rate_value',
    'basket_items_per_order_value', 'users_activated_count',
    'cs_ticket_order_rate_value', 'customer_refunded_order_rate_value',
    'total_provider_active_minutes', 'total_provider_inactive_minutes'
).orderBy('provider_id', 'metric_timestamp_local')

result = izi_data.collect()
output = []
for row in result:
    record = {{}}
    for field in row.__fields__:
        val = row[field]
        if field in ('provider_id', 'city_id'):
            record[field] = int(val) if val is not None else None
        elif field == 'metric_timestamp_local':
            record[field] = str(val)[:10] if val is not None else None
        else:
            record[field] = float(val) if val is not None else None
    output.append(record)

dbutils.fs.put('/FileStore/izi_burger_report_data.json', json.dumps(output, ensure_ascii=False), overwrite=True)
print(f"Done: {{len(output)}} rows written")
"""

    output = run_spark_command(spark_code)
    print(f"Spark output: {output}")

    resp = db_request('/api/2.0/dbfs/read?path=/FileStore/izi_burger_report_data.json')
    data_str = base64.b64decode(resp['data']).decode('utf-8')
    raw_rows = json.loads(data_str)

    # structured[pid][date] = {metric: value}
    structured = {}
    for row in raw_rows:
        pid  = str(row['provider_id'])
        date = row['metric_timestamp_local']
        if pid not in structured:
            structured[pid] = {}
        structured[pid][date] = {k: v for k, v in row.items()
                                 if k not in ('provider_id', 'city_id', 'metric_timestamp_local')}

    return structured, week_starts


# ─── HTML GENERATION ─────────────────────────────────────────────────────────

def generate_html(structured, week_starts):
    """Generate the full HTML report."""
    WEEKS = [w.strftime('%Y-%m-%d') for w in week_starts]
    WEEK_LABELS = [
        f"{w.strftime('%d.%m')}–{(w + datetime.timedelta(days=6)).strftime('%d.%m')}"
        for w in week_starts
    ]

    generated_at = datetime.datetime.now().strftime('%d.%m.%Y %H:%M')

    def sf(val, default=0.0):
        try:
            return float(val) if val is not None else default
        except Exception:
            return default

    def g(pid, w, key):
        return structured.get(pid, {}).get(w, {}).get(key, None)

    def pct(pid, w, key):
        val = g(pid, w, key)
        return f'{sf(val)*100:.1f}%' if val is not None else '—'

    def uah_fmt(val):
        if val is None:
            return '—'
        return f'{sf(val):,.0f}'.replace(',', '\u00a0') + '\u00a0₴'

    def uah(pid, w, key):
        return uah_fmt(g(pid, w, key))

    def num1(pid, w, key):
        val = g(pid, w, key)
        return f'{sf(val):.1f}' if val is not None else '—'

    def num0(pid, w, key):
        val = g(pid, w, key)
        return f'{sf(val):.0f}' if val is not None else '—'

    def get_arr(pid, key, mult=1, rnd=1):
        return [round(sf(g(pid, w, key), 0) * mult, rnd) for w in WEEKS]

    def avail_pct_str(pid, w):
        active   = sf(g(pid, w, 'total_provider_active_minutes'))
        inactive = sf(g(pid, w, 'total_provider_inactive_minutes'))
        total = active + inactive
        return f'{active / total * 100:.1f}%' if total > 0 else '—'

    def avail_arr(pid):
        out = []
        for w in WEEKS:
            active   = sf(g(pid, w, 'total_provider_active_minutes'))
            inactive = sf(g(pid, w, 'total_provider_inactive_minutes'))
            total = active + inactive
            out.append(round(active / total * 100, 1) if total > 0 else 0)
        return out

    # Build chart data
    chart_data = {}
    for pid in PROVIDERS_ORDER:
        chart_data[pid] = {
            'delivered_orders':    get_arr(pid, 'delivered_orders_count', 1, 0),
            'placed_orders':       get_arr(pid, 'placed_orders_count', 1, 0),
            'failed_order_rate':   get_arr(pid, 'failed_order_rate_value', 100, 1),
            'gmv_before':          get_arr(pid, 'total_provider_price_before_discounts', 1, 0),
            'gmv_after':           get_arr(pid, 'total_provider_price_after_discounts', 1, 0),
            'provider_acceptance': get_arr(pid, 'provider_acceptance_rate_value', 100, 1),
            'courier_completion':  get_arr(pid, 'courier_delivery_completion_rate_value', 100, 1),
            'courier_acceptance':  get_arr(pid, 'courier_acceptance_rate_value', 100, 1),
            'bad_order_rate':      get_arr(pid, 'bad_order_rate_value', 100, 1),
            'bad_provider_rating': get_arr(pid, 'bad_provider_rating_rate_value', 100, 1),
            'basket_items':        get_arr(pid, 'basket_items_per_order_value', 1, 1),
            'users_activated':     get_arr(pid, 'users_activated_count', 1, 0),
            'cs_ticket':           get_arr(pid, 'cs_ticket_order_rate_value', 100, 1),
            'customer_refund':     get_arr(pid, 'customer_refunded_order_rate_value', 100, 1),
            'availability':        avail_arr(pid),
        }

    labels_json = json.dumps(WEEK_LABELS)

    def build_table_rows(pid):
        metrics = [
            ('Виконані замовлення',            lambda w: num0(pid, w, 'delivered_orders_count')),
            ('Розміщені замовлення',           lambda w: num0(pid, w, 'placed_orders_count')),
            ('Відсоток невдалих замовлень',    lambda w: pct(pid, w, 'failed_order_rate_value')),
            ('Загальні продажі партнера',      lambda w: uah(pid, w, 'total_provider_price_before_discounts')),
            ('Чисті продажі',                  lambda w: uah(pid, w, 'total_provider_price_after_discounts')),
            ('Прийняття замовлень рестораном', lambda w: pct(pid, w, 'provider_acceptance_rate_value')),
            ("Виконання доставок кур'єром",    lambda w: pct(pid, w, 'courier_delivery_completion_rate_value')),
            ('Погані замовлення',              lambda w: pct(pid, w, 'bad_order_rate_value')),
            ('Погана оцінка ресторану',        lambda w: pct(pid, w, 'bad_provider_rating_rate_value')),
            ("Прийняття замовлень кур'єром",   lambda w: pct(pid, w, 'courier_acceptance_rate_value')),
            ('Позицій у кошику на замовлення', lambda w: num1(pid, w, 'basket_items_per_order_value')),
            ('Нові активовані користувачі',    lambda w: num0(pid, w, 'users_activated_count')),
            ('% звернень до служби підтримки', lambda w: pct(pid, w, 'cs_ticket_order_rate_value')),
            ('Повернення коштів замовникам',   lambda w: pct(pid, w, 'customer_refunded_order_rate_value')),
            ('% доступності (Availability)',   lambda w: avail_pct_str(pid, w)),
        ]
        rows = []
        for metric_name, fn in metrics:
            cells = ''.join(f'<td>{fn(w)}</td>' for w in WEEKS)
            rows.append(f'<tr><td class="metric-name">{metric_name}</td>{cells}</tr>')
        return '\n'.join(rows)

    def build_charts(pid):
        cd = chart_data[pid]
        BOLT_GREEN  = '#34D186'
        BOLT_BLUE   = '#4A90E2'
        BOLT_ORANGE = '#FF6B35'

        chart_defs = [
            ('orders', 'Кількість замовлень', 'Кількість', [
                ('Виконані замовлення', 'delivered_orders', BOLT_GREEN),
                ('Розміщені замовлення', 'placed_orders', '#A8E6CF'),
            ]),
            ('sales', 'Продажі (₴)', '₴', [
                ('Загальні продажі партнера', 'gmv_before', BOLT_GREEN),
                ('Чисті продажі', 'gmv_after', '#A8E6CF'),
            ]),
            ('acceptance', 'Прийняття замовлень (%)', '%', [
                ('Прийняття рестораном', 'provider_acceptance', BOLT_GREEN),
                ("Прийняття кур'єром",   'courier_acceptance',  BOLT_BLUE),
            ]),
            ('quality', 'Показники якості (%)', '%', [
                ('Погані замовлення',       'bad_order_rate',      BOLT_ORANGE),
                ('Погана оцінка ресторану', 'bad_provider_rating', '#FF9F43'),
                ('Невдалі замовлення',      'failed_order_rate',   '#EE5A24'),
            ]),
            ('basket', 'Позицій у кошику', 'к-сть', [
                ('Позицій/замовл.', 'basket_items', BOLT_GREEN),
            ]),
            ('users', 'Нові активовані користувачі', 'к-сть', [
                ('Нові користувачі', 'users_activated', BOLT_GREEN),
            ]),
            ('availability', '% доступності (Availability)', '%', [
                ('% доступності', 'availability', BOLT_GREEN),
            ]),
        ]

        html_parts = []
        for chart_id, title, ylabel, datasets in chart_defs:
            cid = f'chart_{pid}_{chart_id}'
            ds_js = ',\n'.join([
                f'''{{label:{json.dumps(lbl)},data:{json.dumps(cd[key])},backgroundColor:'{col}',borderColor:'{col}',borderWidth:1,borderRadius:4}}'''
                for lbl, key, col in datasets
            ])
            html_parts.append(f'''
<div class="chart-card">
  <h4 class="chart-title">{title}</h4>
  <div class="chart-container"><canvas id="{cid}"></canvas></div>
  <script>
  (function(){{
    var ctx=document.getElementById('{cid}').getContext('2d');
    new Chart(ctx,{{type:'bar',data:{{labels:{labels_json},datasets:[{ds_js}]}},options:{{responsive:true,maintainAspectRatio:true,plugins:{{legend:{{position:'top',labels:{{font:{{size:11}},color:'#1C1F23'}}}},tooltip:{{mode:'index',intersect:false}}}},scales:{{x:{{grid:{{display:false}},ticks:{{font:{{size:10}},color:'#666'}}}},y:{{beginAtZero:true,grid:{{color:'rgba(0,0,0,0.05)'}},ticks:{{font:{{size:10}},color:'#666'}},title:{{display:true,text:{json.dumps(ylabel)},font:{{size:10}},color:'#666'}}}}}}}}}})}};
  }})();
  </script>
</div>''')
        return '\n'.join(html_parts)

    # Build location tabs and sections
    location_navs = []
    location_sections = []

    for i, pid in enumerate(PROVIDERS_ORDER):
        pname    = PROVIDERS[pid]['name']
        paddress = PROVIDERS[pid]['address']
        active   = 'active' if i == 0 else ''

        location_navs.append(
            f'<button class="loc-tab {active}" data-target="loc-{pid}" onclick="showLocation(\'{pid}\')">'
            f'<span class="loc-icon">📍</span>{pname}</button>'
        )

        last_w    = WEEKS[-1]
        delivered = sf(g(pid, last_w, 'delivered_orders_count'), 0)
        sales_val = sf(g(pid, last_w, 'total_provider_price_before_discounts'), 0)
        acceptance= sf(g(pid, last_w, 'provider_acceptance_rate_value'), 0)
        bad_order = sf(g(pid, last_w, 'bad_order_rate_value'), 0)

        week_headers = ''.join(f'<th>{WEEK_LABELS[j]}</th>' for j in range(len(WEEKS)))

        section_html = f'''
<div id="loc-{pid}" class="location-section {active}">
  <div class="location-header">
    <div class="location-title-block">
      <h2 class="location-name">{pname}</h2>
      <div class="location-address"><span>📍</span> {paddress}</div>
    </div>
    <div class="summary-cards">
      <div class="summary-card">
        <div class="summary-value">{int(delivered)}</div>
        <div class="summary-label">Замовлень (останній тиждень)</div>
      </div>
      <div class="summary-card">
        <div class="summary-value">{uah_fmt(sales_val)}</div>
        <div class="summary-label">Продажі (останній тиждень)</div>
      </div>
      <div class="summary-card highlight-green">
        <div class="summary-value">{acceptance*100:.1f}%</div>
        <div class="summary-label">Прийняття замовлень</div>
      </div>
      <div class="summary-card {'highlight-red' if bad_order > 0.1 else 'highlight-green'}">
        <div class="summary-value">{bad_order*100:.1f}%</div>
        <div class="summary-label">Погані замовлення</div>
      </div>
    </div>
  </div>
  <div class="section-block">
    <h3 class="section-title">📊 Метрики по тижнях</h3>
    <div class="table-wrapper">
      <table class="metrics-table">
        <thead><tr><th class="metric-col">Метрика</th>{week_headers}</tr></thead>
        <tbody>
{build_table_rows(pid)}
        </tbody>
      </table>
    </div>
  </div>
  <div class="section-block">
    <h3 class="section-title">📈 Графіки по тижнях</h3>
    <div class="charts-grid">
{build_charts(pid)}
    </div>
  </div>
</div>'''
        location_sections.append(section_html)

    period_start = week_starts[0].strftime('%d.%m.%Y')
    period_end   = (week_starts[-1] + datetime.timedelta(days=6)).strftime('%d.%m.%Y')
    year = datetime.datetime.now().year

    return f'''<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Щотижневий звіт Izi Burger – Харків</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    :root{{--bolt-green:#34D186;--bolt-dark:#1C1F23;--bolt-light-green:#E8FAF2;--bolt-mid-green:#A8E6CF;--bolt-gray:#F5F6F7;--bolt-text:#2C3E50;--bolt-muted:#7F8C8D;--bolt-border:#E0E0E0;--bolt-white:#FFFFFF;--bolt-red:#E74C3C;--bolt-orange:#FF6B35}}
    body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:var(--bolt-gray);color:var(--bolt-text);min-height:100vh}}
    .site-header{{background:var(--bolt-dark);position:sticky;top:0;z-index:100;box-shadow:0 2px 10px rgba(0,0,0,.3)}}
    .header-inner{{max-width:1400px;margin:0 auto;padding:0 24px;display:flex;align-items:stretch}}
    .header-brand{{display:flex;align-items:center;gap:12px;padding:16px 0;text-decoration:none;flex-shrink:0}}
    .bolt-logo{{width:36px;height:36px;background:var(--bolt-green);border-radius:8px;display:flex;align-items:center;justify-content:center;font-weight:900;font-size:18px;color:var(--bolt-dark)}}
    .brand-text{{display:flex;flex-direction:column}}
    .brand-main{{font-size:15px;font-weight:700;color:#fff;line-height:1.2}}
    .brand-sub{{font-size:11px;color:rgba(255,255,255,.5)}}
    .header-divider{{width:1px;background:rgba(255,255,255,.1);margin:12px 20px}}
    .header-title-block{{display:flex;flex-direction:column;justify-content:center;flex:1}}
    .header-report-title{{font-size:16px;font-weight:700;color:#fff}}
    .header-report-sub{{font-size:12px;color:rgba(255,255,255,.5);margin-top:2px}}
    .header-badge{{display:flex;align-items:center;padding:6px 14px;background:rgba(52,209,134,.15);border:1px solid rgba(52,209,134,.3);border-radius:20px;color:var(--bolt-green);font-size:12px;font-weight:600;gap:6px;align-self:center}}
    .dot{{width:6px;height:6px;background:var(--bolt-green);border-radius:50%;animation:pulse 2s infinite}}
    @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.4}}}}
    .location-nav-wrapper{{background:#fff;border-bottom:2px solid var(--bolt-border);position:sticky;top:69px;z-index:90;box-shadow:0 2px 8px rgba(0,0,0,.05)}}
    .location-nav{{max-width:1400px;margin:0 auto;padding:0 24px;display:flex;gap:4px;overflow-x:auto}}
    .loc-tab{{padding:16px 24px;border:none;background:transparent;cursor:pointer;font-size:14px;font-weight:600;color:var(--bolt-muted);white-space:nowrap;display:flex;align-items:center;gap:8px;border-bottom:3px solid transparent;transition:all .2s;position:relative;bottom:-2px}}
    .loc-tab:hover{{color:var(--bolt-dark);background:var(--bolt-gray)}}
    .loc-tab.active{{color:var(--bolt-green);border-bottom-color:var(--bolt-green)}}
    .main-content{{max-width:1400px;margin:0 auto;padding:24px}}
    .location-section{{display:none}}
    .location-section.active{{display:block}}
    .location-header{{background:#fff;border-radius:12px;padding:24px;margin-bottom:20px;box-shadow:0 2px 8px rgba(0,0,0,.06);display:flex;align-items:flex-start;gap:24px;flex-wrap:wrap}}
    .location-title-block{{flex:1;min-width:200px}}
    .location-name{{font-size:22px;font-weight:800;color:var(--bolt-dark);margin-bottom:6px}}
    .location-address{{font-size:13px;color:var(--bolt-muted);display:flex;align-items:center;gap:4px}}
    .summary-cards{{display:flex;gap:12px;flex-wrap:wrap}}
    .summary-card{{background:var(--bolt-gray);border-radius:10px;padding:14px 18px;min-width:140px;text-align:center;border:1px solid var(--bolt-border)}}
    .summary-card.highlight-green{{background:var(--bolt-light-green);border-color:var(--bolt-mid-green)}}
    .summary-card.highlight-red{{background:#FEF0EE;border-color:#FADBD8}}
    .summary-value{{font-size:20px;font-weight:800;color:var(--bolt-dark);margin-bottom:4px}}
    .summary-label{{font-size:11px;color:var(--bolt-muted);line-height:1.3}}
    .section-block{{background:#fff;border-radius:12px;padding:24px;margin-bottom:20px;box-shadow:0 2px 8px rgba(0,0,0,.06)}}
    .section-title{{font-size:16px;font-weight:700;color:var(--bolt-dark);margin-bottom:20px;padding-bottom:12px;border-bottom:2px solid var(--bolt-light-green);display:flex;align-items:center;gap:8px}}
    .table-wrapper{{overflow-x:auto;border-radius:8px;border:1px solid var(--bolt-border)}}
    .metrics-table{{width:100%;border-collapse:collapse;font-size:13px}}
    .metrics-table thead th{{background:var(--bolt-dark);color:#fff;padding:10px 14px;text-align:center;font-weight:600;font-size:12px;white-space:nowrap}}
    .metrics-table thead th.metric-col{{text-align:left;min-width:240px}}
    .metrics-table tbody tr{{border-bottom:1px solid var(--bolt-border);transition:background .15s}}
    .metrics-table tbody tr:hover{{background:var(--bolt-light-green)}}
    .metrics-table tbody tr:nth-child(even){{background:var(--bolt-gray)}}
    .metrics-table tbody tr:nth-child(even):hover{{background:var(--bolt-light-green)}}
    .metrics-table td{{padding:10px 14px;text-align:center;color:var(--bolt-text);white-space:nowrap}}
    .metrics-table td.metric-name{{text-align:left;font-weight:600;color:var(--bolt-dark);font-size:13px;white-space:normal;min-width:240px}}
    .charts-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(480px,1fr));gap:20px}}
    .chart-card{{background:#fff;border-radius:10px;padding:20px;border:1px solid var(--bolt-border)}}
    .chart-title{{font-size:13px;font-weight:700;color:var(--bolt-dark);margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid var(--bolt-border)}}
    .chart-container{{position:relative;height:220px}}
    .site-footer{{background:var(--bolt-dark);color:rgba(255,255,255,.5);text-align:center;padding:20px;font-size:12px;margin-top:40px}}
    .site-footer a{{color:var(--bolt-green);text-decoration:none}}
    .period-badge{{display:inline-flex;align-items:center;gap:6px;background:var(--bolt-light-green);color:#1A7A4C;font-size:12px;font-weight:600;padding:5px 12px;border-radius:20px;margin-bottom:20px;border:1px solid var(--bolt-mid-green)}}
    @media(max-width:768px){{.header-inner{{padding:0 16px}}.main-content{{padding:16px}}.location-header{{flex-direction:column}}.charts-grid{{grid-template-columns:1fr}}.summary-cards{{justify-content:center}}}}
  </style>
</head>
<body>
<header class="site-header">
  <div class="header-inner">
    <a href="#" class="header-brand">
      <div class="bolt-logo">⚡</div>
      <div class="brand-text"><span class="brand-main">Bolt Food</span><span class="brand-sub">Partner Reports</span></div>
    </a>
    <div class="header-divider"></div>
    <div class="header-title-block">
      <div class="header-report-title">Щотижневий звіт IZI BURGER – Харків</div>
      <div class="header-report-sub">Vendor External Report 3.0 · 8 тижнів · Оновлено: {generated_at}</div>
    </div>
    <div class="header-badge"><span class="dot"></span>Автооновлення щопонеділка</div>
  </div>
</header>
<nav class="location-nav-wrapper">
  <div class="location-nav">{''.join(location_navs)}</div>
</nav>
<main class="main-content">
  <div class="period-badge">📅 Звітний період: {period_start} – {period_end} (8 повних тижнів)</div>
  {''.join(location_sections)}
</main>
<footer class="site-footer">
  <p>© {year} Bolt Food · Щотижневий звіт IZI BURGER (Харків) · Джерело: Databricks <code>main.ng_delivery.fact_provider_weekly</code></p>
  <p style="margin-top:6px">Автоматично оновлюється кожного понеділка о 13:30 за київським часом</p>
</footer>
<script>
function showLocation(pid){{
  document.querySelectorAll('.location-section').forEach(s=>s.classList.remove('active'));
  document.querySelectorAll('.loc-tab').forEach(t=>t.classList.remove('active'));
  document.getElementById('loc-'+pid).classList.add('active');
  document.querySelector('[data-target="loc-'+pid+'"]').classList.add('active');
}}
</script>
</body>
</html>'''


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    if not DATABRICKS_TOKEN:
        print("ERROR: DATABRICKS_TOKEN environment variable not set")
        sys.exit(1)

    print("Fetching data from Databricks...")
    structured, week_starts = fetch_data()

    print("Generating HTML report...")
    html = generate_html(structured, week_starts)

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"Report saved to: {OUTPUT_PATH} ({len(html)} bytes)")


if __name__ == '__main__':
    main()
