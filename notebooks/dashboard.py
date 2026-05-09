import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Thailand Election 2026 | Chaiyaphum 2",
    page_icon="🗳️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Palette ────────────────────────────────────────────────────────────────────
ORANGE     = "#FF7A3D"
LIGHT_OG   = "#FBBF5D"
DARK_OG    = "#E85D1F"
TEAL       = "#3DD6C6"
VIOLET     = "#9B8CFF"
BG         = "#0B0D12"
CARD_BG    = "#141821"
PANEL_BG   = "#10141C"
BORDER     = "#273142"
WHITE      = "#F8FAFC"
LGRAY      = "#A7B0C0"
MUTED      = "#697386"
GRID       = "#263041"
CHART_BG   = "rgba(0,0,0,0)"
PALETTE    = [ORANGE, TEAL, LIGHT_OG, VIOLET, "#67A6FF",
              "#F87171", "#34D399", "#C084FC", "#FACC15", "#FB7185"]

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
  :root {{
      --bg: {BG};
      --panel: {PANEL_BG};
      --card: {CARD_BG};
      --border: {BORDER};
      --text: {WHITE};
      --muted: {LGRAY};
      --accent: {ORANGE};
      --accent-2: {TEAL};
  }}
  .stApp {{
      background:
        radial-gradient(circle at top left, rgba(255,122,61,0.14), transparent 32rem),
        radial-gradient(circle at top right, rgba(61,214,198,0.10), transparent 30rem),
        linear-gradient(180deg, #0B0D12 0%, #0E1219 48%, #0B0D12 100%);
      color: var(--text);
  }}
  .block-container {{
      padding-top: 1.2rem;
      padding-right: clamp(1.35rem, 2.8vw, 3rem);
      padding-bottom: 2.5rem;
      padding-left: clamp(1.35rem, 2.8vw, 3rem);
      max-width: 1500px;
      overflow-x: hidden;
  }}
  header[data-testid="stHeader"] {{
      background: transparent;
      height: 2.75rem;
      pointer-events: auto;
  }}
  [data-testid="stToolbar"] {{
      visibility: hidden;
      pointer-events: none;
  }}
  header[data-testid="stHeader"] [data-testid="stToolbar"] > div > div:last-child {{
      display: none !important;
  }}
  #MainMenu, footer {{
      visibility: hidden;
      height: 0;
  }}
  [data-testid="stExpandSidebarButton"],
  [data-testid="stSidebarCollapsedControl"],
  [data-testid="stSidebarCollapseButton"] {{
      visibility: visible !important;
      opacity: 1 !important;
      pointer-events: auto !important;
      z-index: 999999 !important;
  }}
  [data-testid="stExpandSidebarButton"] {{
      display: inline-flex !important;
      align-items: center !important;
      justify-content: center !important;
      position: fixed !important;
      top: 0.72rem !important;
      left: 0.72rem !important;
      width: 2.25rem !important;
      height: 2.25rem !important;
      background: rgba(20,24,33,0.96) !important;
      border: 1px solid rgba(255,255,255,0.18) !important;
      border-radius: 8px !important;
      box-shadow: 0 12px 28px rgba(0,0,0,0.32);
  }}
  [data-testid="stExpandSidebarButton"] svg {{
      color: {WHITE} !important;
  }}
  [data-testid="stSidebarCollapsedControl"] button,
  [data-testid="stSidebarCollapseButton"] button {{
      background: rgba(20,24,33,0.94) !important;
      border: 1px solid rgba(255,255,255,0.14) !important;
      border-radius: 8px !important;
      color: {WHITE} !important;
      box-shadow: 0 10px 26px rgba(0,0,0,0.24);
  }}
  section[data-testid="stSidebar"] {{
      background: linear-gradient(180deg, #10141C 0%, #0B0D12 100%);
      border-right: 1px solid var(--border);
      -webkit-user-select: none;
      user-select: none;
  }}
  section[data-testid="stSidebar"] *,
  [data-testid="stExpandSidebarButton"],
  [data-testid="stSidebarCollapsedControl"],
  [data-testid="stSidebarCollapseButton"],
  .stTabs [data-baseweb="tab"],
  .stSlider {{
      -webkit-user-select: none !important;
      user-select: none !important;
  }}
  [data-testid="stExpandSidebarButton"],
  [data-testid="stExpandSidebarButton"] *,
  [data-testid="stSidebarCollapsedControl"],
  [data-testid="stSidebarCollapsedControl"] *,
  [data-testid="stSidebarCollapseButton"],
  [data-testid="stSidebarCollapseButton"] *,
  section[data-testid="stSidebar"] div[data-baseweb="select"],
  section[data-testid="stSidebar"] div[data-baseweb="select"] *,
  .stTabs [data-baseweb="tab"],
  .stTabs [data-baseweb="tab"] *,
  .stSlider,
  .stSlider * {{
      cursor: pointer !important;
  }}
  section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"],
  section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] *,
  section[data-testid="stSidebar"] label,
  section[data-testid="stSidebar"] label * {{
      cursor: default !important;
  }}
  section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {{
      color: var(--muted);
  }}
  /* Metric cards */
  [data-testid="metric-container"] {{
      background: linear-gradient(180deg, rgba(255,255,255,0.055), rgba(255,255,255,0.025));
      border: 1px solid rgba(255,255,255,0.10);
      border-radius: 8px;
      padding: 16px 18px;
      box-shadow: 0 18px 40px rgba(0,0,0,0.20);
  }}
  [data-testid="metric-container"] label {{
      color: {LGRAY} !important;
      font-size: 0.78rem !important;
      letter-spacing: 0 !important;
  }}
  [data-testid="stMetricValue"] {{
      color: {WHITE} !important;
      font-size: 1.55rem !important;
      font-weight: 760 !important;
  }}
  /* Tabs */
  .stTabs [data-baseweb="tab-list"] {{
      gap: 4px;
      border-bottom: 1px solid var(--border);
      padding-bottom: 0;
      overflow-x: auto;
      scrollbar-width: thin;
  }}
  .stTabs [data-baseweb="tab"] {{
      background: transparent;
      border-radius: 6px 6px 0 0;
      padding: 10px 12px;
      color: {LGRAY};
      font-size: 0.86rem;
      font-weight: 650;
      border: 1px solid transparent;
      border-bottom: 0;
      flex: 0 0 auto;
  }}
  .stTabs [aria-selected="true"] {{
      background: rgba(255,122,61,0.16) !important;
      border-color: rgba(255,122,61,0.42) !important;
      color: {WHITE} !important;
  }}
  /* Headings */
  h1, h2, h3, h4 {{ color: {WHITE} !important; letter-spacing: 0 !important; }}
  h2, h3 {{ margin-top: 0.7rem !important; }}
  /* Sidebar select labels */
  .stSelectbox > label, .stSlider > label {{ color: {LGRAY} !important; }}
  div[data-baseweb="select"] > div,
  div[data-baseweb="select"] > div:hover,
  div[data-baseweb="select"] > div:focus,
  div[data-baseweb="select"] > div:focus-within {{
      background: rgba(255,255,255,0.045);
      border-color: var(--border) !important;
      border-radius: 8px;
      outline: none !important;
      box-shadow: none !important;
  }}
  div[data-baseweb="select"] input {{
      caret-color: transparent !important;
      cursor: pointer !important;
      outline: none !important;
      box-shadow: none !important;
      user-select: none !important;
      -webkit-user-select: none !important;
  }}
  div[data-baseweb="select"] input::selection {{
      background: transparent !important;
      color: inherit !important;
  }}
  div[data-baseweb="select"] [role="combobox"],
  div[data-baseweb="select"] [role="combobox"]:focus,
  div[data-baseweb="select"] [role="combobox"]:focus-visible,
  div[data-baseweb="select"] [aria-expanded="true"] {{
      outline: none !important;
      box-shadow: none !important;
      border-color: var(--border) !important;
  }}
  button:focus-visible,
  [role="tab"]:focus-visible {{
      outline: 2px solid rgba(255,122,61,0.78) !important;
      outline-offset: 2px !important;
      box-shadow: 0 0 0 4px rgba(255,122,61,0.12) !important;
  }}
  /* Divider */
  hr {{ border-color: {BORDER}; opacity: 0.75; }}
  .stDataFrame {{
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow: hidden;
  }}
  div[data-testid="stPlotlyChart"] {{
      background: linear-gradient(180deg, rgba(255,255,255,0.038), rgba(255,255,255,0.018));
      border: 1px solid rgba(255,255,255,0.085);
      border-radius: 8px;
      padding: 0;
      box-shadow: none;
      overflow: hidden;
   }}
  div[data-testid="stPlotlyChart"] > div {{
      border-radius: 8px;
  }}
  .dashboard-hero {{
      background:
        linear-gradient(135deg, rgba(255,122,61,0.18), rgba(61,214,198,0.08) 54%, rgba(155,140,255,0.10)),
        rgba(16,20,28,0.92);
      border: 1px solid rgba(255,255,255,0.10);
      border-radius: 10px;
      padding: 24px 28px;
      box-shadow: 0 24px 60px rgba(0,0,0,0.22);
      margin-bottom: 10px;
  }}
  .dashboard-hero h1 {{
      font-size: 2.15rem;
      line-height: 1.12;
      margin: 0 0 8px 0;
      font-weight: 800;
  }}
  .dashboard-hero p {{
      color: {LGRAY};
      margin: 0;
      font-size: 0.96rem;
  }}
  .version-chip {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      margin-top: 16px;
      padding: 6px 10px;
      border-radius: 7px;
      background: rgba(255,122,61,0.16);
      border: 1px solid rgba(255,122,61,0.36);
      color: {WHITE};
      font-size: 0.82rem;
      font-weight: 700;
  }}
  .sidebar-card {{
      background: rgba(255,255,255,0.045);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 13px 14px;
      color: {LGRAY};
      font-size: 0.8rem;
      line-height: 1.75;
  }}
  .sidebar-card strong {{
      color: {WHITE};
  }}
  .status-strip {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      margin: -2px 0 14px;
   }}
  .status-pill {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border: 1px solid rgba(255,255,255,0.10);
      border-radius: 7px;
      background: rgba(255,255,255,0.045);
      color: {LGRAY};
      padding: 7px 10px;
      font-size: 0.78rem;
      font-weight: 650;
      line-height: 1.2;
   }}
  .status-pill strong {{
      color: {WHITE};
      font-weight: 760;
   }}
  .status-pill.warning {{
      border-color: rgba(255,122,61,0.34);
      background: rgba(255,122,61,0.12);
      color: {WHITE};
   }}
  .empty-state {{
      border: 1px dashed rgba(255,255,255,0.18);
      border-radius: 8px;
      background: rgba(255,255,255,0.035);
      padding: 18px;
      color: {LGRAY};
      font-size: 0.9rem;
   }}
  .empty-state strong {{
      display: block;
      color: {WHITE};
      font-size: 1rem;
      margin-bottom: 4px;
   }}
  .insight-card {{
      background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.025));
      border: 1px solid rgba(255,255,255,0.10);
      border-radius: 8px;
      padding: 19px;
      box-shadow: 0 18px 45px rgba(0,0,0,0.20);
  }}
  .kpi-grid {{
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
      margin: 6px 0 12px;
  }}
  .kpi-card {{
      background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.025));
      border: 1px solid rgba(255,255,255,0.10);
      border-radius: 8px;
      padding: 15px 16px;
      box-shadow: 0 18px 40px rgba(0,0,0,0.18);
  }}
  .kpi-label {{
      color: {LGRAY};
      font-size: 0.78rem;
      font-weight: 650;
      margin-bottom: 7px;
      white-space: nowrap;
  }}
  .kpi-value {{
      color: {WHITE};
      font-size: 1.5rem;
      font-weight: 820;
      line-height: 1.1;
  }}
  .kpi-card:nth-child(4) .kpi-value,
  .kpi-card:nth-child(5) .kpi-value {{
      color: {ORANGE};
  }}
  @media (max-width: 1100px) {{
      .kpi-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .dashboard-hero h1 {{ font-size: 1.75rem; }}
  }}
  @media (max-width: 720px) {{
      .kpi-grid {{ grid-template-columns: 1fr; }}
      .dashboard-hero {{ padding: 20px; }}
      .stTabs [data-baseweb="tab"] {{ padding: 8px 10px; font-size: 0.8rem; }}
  }}
</style>
""", unsafe_allow_html=True)

st.html(
    """
    <script>
      const applyReadonlySelectboxes = () => {
        document.querySelectorAll('input[role="combobox"]').forEach((input) => {
          if (input.dataset.readonlySelectApplied === "true") return;
          input.dataset.readonlySelectApplied = "true";
          input.setAttribute("readonly", "readonly");
          input.setAttribute("inputmode", "none");
          input.addEventListener("keydown", (event) => {
            const allowed = ["Tab", "Escape", "Enter", "ArrowDown", "ArrowUp", "ArrowLeft", "ArrowRight"];
            if (!allowed.includes(event.key)) event.preventDefault();
          }, true);
          input.addEventListener("paste", (event) => event.preventDefault(), true);
          input.addEventListener("drop", (event) => event.preventDefault(), true);
        });
      };
      applyReadonlySelectboxes();
      new MutationObserver(applyReadonlySelectboxes).observe(document.body, {
        childList: true,
        subtree: true,
      });
    </script>
    """,
    unsafe_allow_javascript=True,
    width="content",
)

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE     = Path(__file__).resolve().parent.parent
CLEAN    = BASE / "data" / "clean_data"
EXTERNAL = BASE / "data" / "external"


# ── Loaders ────────────────────────────────────────────────────────────────────
@st.cache_data
def load_data():
    station   = pd.read_csv(CLEAN / "5_18_station.csv",      encoding="utf-8-sig")
    votes     = pd.read_csv(CLEAN / "5_18_votes.csv",         encoding="utf-8-sig")
    p_station = pd.read_csv(CLEAN / "5_18_party_station.csv", encoding="utf-8-sig")
    p_votes   = pd.read_csv(CLEAN / "5_18_party_vote.csv",    encoding="utf-8-sig")
    cand_ref  = pd.read_csv(EXTERNAL / "candidates.csv",      encoding="utf-8-sig")
    party_ref = pd.read_csv(EXTERNAL / "parties.csv",         encoding="utf-8-sig")
    v4_votes  = pd.read_csv(CLEAN / "5_18_votes_KNNImputed.csv",      encoding="utf-8-sig")
    v4_party  = pd.read_csv(CLEAN / "5_18_party_vote_KNNImputed.csv", encoding="utf-8-sig")
    
    try:
        missing_st = pd.read_csv(CLEAN / "5_18_missing_stations_report.csv", encoding="utf-8-sig")
    except FileNotFoundError:
        missing_st = pd.DataFrame()
        
    try:
        missing_pst = pd.read_csv(CLEAN / "5_18_party_missing_stations_report.csv", encoding="utf-8-sig")
    except FileNotFoundError:
        missing_pst = pd.DataFrame()
        
    return station, votes, p_station, p_votes, cand_ref, party_ref, v4_votes, v4_party, missing_st, missing_pst


try:
    station_df, votes_df, p_station_df, p_votes_df, cand_ref, party_ref, v4_votes_df, v4_party_df, missing_st_df, missing_pst_df = load_data()
except FileNotFoundError as e:
    st.error(f"⚠️ Data files not found: {e}")
    st.stop()

# ── V3 ground truth ────────────────────────────────────────────────────────────
v3_cand = (
    cand_ref[["candidate_name", "party_name", "votes_reference"]]
    .rename(columns={"candidate_name": "entity_name",
                     "votes_reference": "votes"})
    .sort_values("votes", ascending=False)
    .reset_index(drop=True)
)
v3_party = (
    party_ref[["party_name", "votes_reference_constituency"]]
    .rename(columns={"party_name": "entity_name",
                     "votes_reference_constituency": "votes"})
    .sort_values("votes", ascending=False)
    .reset_index(drop=True)
)

ref_cand_total  = v3_cand["votes"].sum()
ref_party_total = v3_party["votes"].sum()

# ── V2 proportional scale ──────────────────────────────────────────────────────
scale_c = ref_cand_total  / votes_df["votes"].sum()
scale_p = ref_party_total / p_votes_df["votes"].sum()

v2_votes   = votes_df.copy();   v2_votes["votes"]   = (v2_votes["votes"]   * scale_c).round()
v2_p_votes = p_votes_df.copy(); v2_p_votes["votes"] = (v2_p_votes["votes"] * scale_p).round()


# ── Helpers ────────────────────────────────────────────────────────────────────
def styled_bar(df, x, y, title, color=ORANGE, height=400):
    fig = px.bar(df, x=x, y=y, orientation="h", title=title,
                 color_discrete_sequence=[color])
    x_title = "Votes" if x == "votes" else x.replace("_", " ").title()
    fig.update_layout(
        plot_bgcolor=CHART_BG, paper_bgcolor=CHART_BG,
        font_color=WHITE, title_font_color=WHITE,
        title_font=dict(size=18, family="Inter, system-ui, sans-serif"),
        height=height,
        xaxis=dict(
            gridcolor=GRID,
            showgrid=True,
            zeroline=False,
            title=dict(text=x_title, standoff=12),
            automargin=True,
        ),
        yaxis=dict(gridcolor=GRID, categoryorder="total ascending", zeroline=False, title="", automargin=True),
        margin=dict(l=14, r=40, t=52, b=54),
        bargap=0.28,
        hoverlabel=dict(bgcolor=PANEL_BG, font_color=WHITE, bordercolor=BORDER),
    )
    fig.update_traces(hovertemplate="%{y}: <b>%{x:,.0f}</b> votes", marker_line_width=0)
    return fig


PLOTLY_CONFIG = {
    "displayModeBar": False,
    "displaylogo": False,
    "responsive": True,
    "modeBarButtonsToRemove": ["lasso2d", "select2d"],
}


def empty_state(title, body):
    st.markdown(
        f"<div class='empty-state'><strong>{title}</strong>{body}</div>",
        unsafe_allow_html=True,
    )


def split_validation_flags(flags: pd.Series) -> pd.Series:
    if flags is None or flags.empty:
        return pd.Series(dtype="string")
    split = flags.dropna().astype(str).str.split(r"[;|]", regex=True).explode().str.strip()
    return split[split.ne("")]


def apply_geo_filter(s_df, v_df, pv_df, ps_df, district, subdistrict):
    if district != "All":
        codes = s_df[s_df["district"] == district]["station_code"].unique()
        s_df  = s_df[s_df["district"] == district]
        v_df  = v_df[v_df["station_code"].isin(codes)]
        pv_df = pv_df[pv_df["station_code"].isin(codes)]
        ps_df = ps_df[ps_df["station_code"].isin(codes)]
    if subdistrict != "All":
        codes = s_df[s_df["subdistrict"] == subdistrict]["station_code"].unique()
        s_df  = s_df[s_df["subdistrict"] == subdistrict]
        v_df  = v_df[v_df["station_code"].isin(codes)]
        pv_df = pv_df[pv_df["station_code"].isin(codes)]
        ps_df = ps_df[ps_df["station_code"].isin(codes)]
    return s_df, v_df, pv_df, ps_df


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        f"<h2 style='color:{WHITE}; margin-bottom:0'>Election 2026</h2>"
        f"<p style='color:{LGRAY}; margin-top:3px; font-size:0.86rem'>Chaiyaphum Constituency 2</p>",
        unsafe_allow_html=True,
    )
    st.divider()

    version = st.selectbox(
        "Data Version",
        ["V1 — OCR + Imputed", "V2 — Proportional Scale", "V3 — Ground Truth", "V4 — KNN Imputed", "🔀 Compare All"],
    )
    ver = version.split("—")[0].strip()

    st.divider()

    districts = ["All"] + sorted(station_df["district"].dropna().unique())
    sel_district = st.selectbox("District", districts)

    sub_pool = (
        station_df[station_df["district"] == sel_district]["subdistrict"]
        if sel_district != "All"
        else station_df["subdistrict"]
    )
    subs = ["All"] + sorted(sub_pool.dropna().unique())
    sel_sub = st.selectbox("Sub-district", subs)

    st.divider()
    cov_c = votes_df["votes"].sum() / ref_cand_total * 100
    cov_p = p_votes_df["votes"].sum() / ref_party_total * 100
    st.markdown(f"""
    <div class='sidebar-card'>
      <strong>OCR Coverage</strong><br>
      Constituency&nbsp;&nbsp;{cov_c:.1f}%<br>
      Party-list&nbsp;&nbsp;&nbsp;&nbsp;{cov_p:.1f}%<br>
      Stations&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{len(station_df)} / 341
    </div>
    """, unsafe_allow_html=True)


# ── Apply geo filter ───────────────────────────────────────────────────────────
f_st, f_v, f_pv, f_ps = apply_geo_filter(
    station_df, votes_df, p_votes_df, p_station_df, sel_district, sel_sub
)
_, f_v2, f_v2p, _ = apply_geo_filter(
    station_df, v2_votes, v2_p_votes, p_station_df, sel_district, sel_sub
)
_, f_v4, f_v4p, _ = apply_geo_filter(
    station_df, v4_votes_df, v4_party_df, p_station_df, sel_district, sel_sub
)

if f_st.empty:
    empty_state(
        "No stations match the selected filters",
        "Try selecting All for District or Sub-district.",
    )
    st.stop()

# Active vote dfs for single-version tabs
if ver == "V1":
    av, apv = f_v, f_pv
elif ver == "V2":
    av, apv = f_v2, f_v2p
elif ver == "V4":
    av, apv = f_v4, f_v4p
elif ver == "V3":
    av, apv = v3_cand, v3_party   # province-level only
else:
    av, apv = f_v, f_pv           # Compare All → use V1 as base


# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class='dashboard-hero'>
  <h1>Thailand General Election 2026</h1>
  <p>
    Chaiyaphum Province · Constituency 2 · interactive election dashboard
  </p>
  <span class='version-chip'>Current view · {ver}</span>
</div>
""", unsafe_allow_html=True)

st.divider()

# ── KPI row ────────────────────────────────────────────────────────────────────
total_elig   = f_st["eligible_voters"].sum()
total_pres   = f_st["voters_present"].sum()
turnout_pct  = total_pres / total_elig * 100 if total_elig else 0
total_used   = f_st["ballots_used"].sum()
spoiled_pct  = f_st["ballots_spoiled"].sum() / total_used * 100 if total_used else 0

st.markdown(f"""
<div class='kpi-grid'>
  <div class='kpi-card'>
    <div class='kpi-label'>Stations</div>
    <div class='kpi-value'>{len(f_st):,}</div>
  </div>
  <div class='kpi-card'>
    <div class='kpi-label'>Eligible Voters</div>
    <div class='kpi-value'>{total_elig:,.0f}</div>
  </div>
  <div class='kpi-card'>
    <div class='kpi-label'>Voters Present</div>
    <div class='kpi-value'>{total_pres:,.0f}</div>
  </div>
  <div class='kpi-card'>
    <div class='kpi-label'>Turnout</div>
    <div class='kpi-value'>{turnout_pct:.2f}%</div>
  </div>
  <div class='kpi-card'>
    <div class='kpi-label'>Spoiled Ballots</div>
    <div class='kpi-value'>{spoiled_pct:.2f}%</div>
  </div>
</div>
""", unsafe_allow_html=True)

scope_label = "All areas"
if sel_district != "All" and sel_sub != "All":
    scope_label = f"{sel_district} · {sel_sub}"
elif sel_district != "All":
    scope_label = sel_district
elif sel_sub != "All":
    scope_label = sel_sub

scope_warning = (
    "<span class='status-pill warning'>V3 reference totals are constituency-level; station filters affect station KPIs only.</span>"
    if ver == "V3" else ""
)

st.markdown(f"""
<div class='status-strip'>
  <span class='status-pill'>Scope <strong>{scope_label}</strong></span>
  <span class='status-pill'>Stations shown <strong>{len(f_st):,}</strong></span>
  <span class='status-pill'>Data view <strong>{ver}</strong></span>
  {scope_warning}
</div>
""", unsafe_allow_html=True)

st.divider()

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
    "Turnout", "Candidates", "Parties", "Split Vote",
    "Station Size", "Versions", "Quality", "Missing"
])


# ─────────────────────────── TAB 1 · TURNOUT ──────────────────────────────────
with tab1:
    if ver == "V3":
        st.info("Station-level turnout is only available for V1 / V2 / V4.")
    else:
        turnout_chart_height = 560
        col_a, col_b = st.columns(2)

        with col_a:
            sub_turn = (
                f_st.groupby("subdistrict")
                .agg(present=("voters_present", "sum"),
                     eligible=("eligible_voters", "sum"))
                .assign(pct=lambda d: d["present"] / d["eligible"] * 100)
                .reset_index().sort_values("pct")
            )
            fig = px.bar(sub_turn, x="pct", y="subdistrict", orientation="h",
                         title="Turnout % by Sub-district",
                         color="pct", color_continuous_scale=[PANEL_BG, ORANGE])
            fig.update_layout(
                plot_bgcolor=CHART_BG, paper_bgcolor=CHART_BG,
                font_color=WHITE, title_font_color=WHITE,
                coloraxis_showscale=False,
                xaxis=dict(gridcolor=GRID, title="Turnout %", zeroline=False),
                yaxis=dict(gridcolor=GRID, title="", zeroline=False),
                height=turnout_chart_height,
                margin=dict(l=10, r=14, t=54, b=58),
                hoverlabel=dict(bgcolor=PANEL_BG, font_color=WHITE, bordercolor=BORDER),
            )
            fig.update_traces(hovertemplate="%{y}: <b>%{x:.1f}%</b>")
            st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)

        with col_b:
            dist_share = (
                f_st.groupby("district")["voters_present"].sum().reset_index()
            )
            fig2 = px.pie(dist_share, values="voters_present", names="district",
                          title="Voter Share by District",
                          color_discrete_sequence=PALETTE, hole=0.4)
            fig2.update_layout(
                plot_bgcolor=CHART_BG, paper_bgcolor=CHART_BG,
                font_color=WHITE, title_font_color=WHITE,
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.14,
                    xanchor="center",
                    x=0.5,
                    font=dict(color=WHITE),
                ),
                height=turnout_chart_height,
                margin=dict(l=18, r=18, t=54, b=92),
                hoverlabel=dict(bgcolor=PANEL_BG, font_color=WHITE, bordercolor=BORDER),
                uniformtext_minsize=11,
                uniformtext_mode="hide",
            )
            fig2.update_traces(
                domain=dict(x=[0.08, 0.92], y=[0.22, 0.96]),
                textposition="inside",
                textinfo="percent",
            )
            st.plotly_chart(fig2, width="stretch", config=PLOTLY_CONFIG)

        # Ballot health
        st.subheader("Ballot Breakdown")
        ballot_df = pd.DataFrame({
            "Type":  ["Good Ballots", "Spoiled", "No Vote"],
            "Count": [f_st["ballots_good"].sum(),
                      f_st["ballots_spoiled"].sum(),
                      f_st["ballots_no_vote"].sum()],
        })
        fig3 = px.pie(ballot_df, values="Count", names="Type",
                      color_discrete_sequence=[ORANGE, "#555", LIGHT_OG], hole=0.48)
        fig3.update_layout(
            plot_bgcolor=CHART_BG, paper_bgcolor=CHART_BG,
            font_color=WHITE,
            title=dict(text="", font=dict(color=WHITE)),
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.08,
                xanchor="center",
                x=0.5,
                font=dict(color=WHITE),
            ),
            height=360,
            margin=dict(l=12, r=12, t=8, b=66),
            hoverlabel=dict(bgcolor=PANEL_BG, font_color=WHITE, bordercolor=BORDER),
            uniformtext_minsize=11,
            uniformtext_mode="hide",
        )
        fig3.update_traces(
            domain=dict(x=[0.08, 0.92], y=[0.16, 0.98]),
            textposition="inside",
            textinfo="percent",
        )
        _, ballot_col, _ = st.columns([1, 2, 1])
        with ballot_col:
            st.plotly_chart(fig3, width="stretch", config=PLOTLY_CONFIG)


# ─────────────────────────── TAB 2 · CANDIDATES ───────────────────────────────
with tab2:
    if ver == "V3":
        cand_agg = v3_cand[["entity_name", "votes"]].copy()
    else:
        cand_agg = (
            av.groupby("entity_name")["votes"].sum()
            .reset_index().sort_values("votes", ascending=False)
        )

    if cand_agg.empty:
        empty_state("No candidate votes", "The selected filters returned no candidate vote rows.")
    else:
        col_a, col_b = st.columns([3, 1])
        with col_a:
            fig = styled_bar(cand_agg, "votes", "entity_name",
                             f"Constituency Votes ({ver})", color=ORANGE, height=380)
            st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)

        with col_b:
            if len(cand_agg) >= 2:
                top2   = cand_agg.sort_values("votes", ascending=False).iloc[:2].reset_index(drop=True)
                winner = top2.loc[0, "entity_name"]
                margin = int(top2.loc[0, "votes"] - top2.loc[1, "votes"])
                total  = int(cand_agg["votes"].sum())
                st.markdown(f"""
                <div class='insight-card' style='text-align:center;'>
                  <div style='color:{LGRAY}; font-size:0.78rem; font-weight:700;'>Winner</div>
                  <div style='color:{WHITE}; font-size:1.04rem; font-weight:760;
                              margin:8px 0'>{winner}</div>
                  <hr>
                  <div style='color:{LGRAY}; font-size:0.78rem'>Victory Margin</div>
                  <div style='color:{ORANGE}; font-size:1.58rem;
                              font-weight:820'>{margin:,}</div>
                  <div style='color:{LGRAY}; font-size:0.78rem'>
                    {margin/total*100:.1f}% of total votes
                  </div>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)
            st.dataframe(
                cand_agg.rename(columns={"entity_name": "Candidate", "votes": "Votes"})
                        .assign(Votes=lambda d: d["Votes"].apply(lambda v: f"{v:,.0f}")),
                width="stretch", hide_index=True,
            )


# ─────────────────────────── TAB 3 · PARTIES ──────────────────────────────────
with tab3:
    if ver == "V3":
        party_agg = v3_party[["entity_name", "votes"]].copy()
    else:
        party_agg = (
            apv.groupby("entity_name")["votes"].sum()
            .reset_index().sort_values("votes", ascending=False)
        )

    top_n = st.slider("Show top N parties", 5, 20, 10, key="party_slider")
    party_top = party_agg.head(top_n)

    if party_top.empty:
        empty_state("No party-list votes", "The selected filters returned no party-list vote rows.")
    else:
        col_a, col_b = st.columns([3, 2])
        with col_a:
            fig = styled_bar(party_top, "votes", "entity_name",
                             f"Party-List Votes — Top {top_n} ({ver})",
                             color=LIGHT_OG, height=420)
            st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)

        with col_b:
            fig2 = px.pie(party_top, values="votes", names="entity_name",
                          title=f"Share — Top {top_n}",
                          color_discrete_sequence=PALETTE, hole=0.35)
            fig2.update_layout(
                plot_bgcolor=CHART_BG, paper_bgcolor=CHART_BG,
                font_color=WHITE, title_font_color=WHITE,
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="top",
                    y=-0.08,
                    xanchor="center",
                    x=0.5,
                    font=dict(color=WHITE, size=11),
                    itemwidth=30,
                ),
                height=470,
                margin=dict(l=10, r=10, t=52, b=116),
                hoverlabel=dict(bgcolor=PANEL_BG, font_color=WHITE, bordercolor=BORDER),
                uniformtext_minsize=10,
                uniformtext_mode="hide",
            )
            fig2.update_traces(
                domain=dict(x=[0.06, 0.94], y=[0.26, 0.96]),
                textposition="inside",
                textinfo="percent",
                insidetextorientation="radial",
            )
            st.plotly_chart(fig2, width="stretch", config=PLOTLY_CONFIG)

        st.dataframe(
            party_top.rename(columns={"entity_name": "Party", "votes": "Votes"})
                     .assign(Votes=lambda d: d["Votes"].apply(lambda v: f"{v:,.0f}")),
            width="stretch", hide_index=True,
        )


# ─────────────────────────── TAB 4 · SPLIT-VOTING ─────────────────────────────
with tab4:
    if ver == "V3":
        cand_by_p = (
            v3_cand[["party_name", "votes"]]
            .rename(columns={"party_name": "Party", "votes": "Candidate_Votes"})
        )
        pty_by_p = (
            v3_party[["entity_name", "votes"]]
            .rename(columns={"entity_name": "Party", "votes": "Party_List_Votes"})
        )
    else:
        cand_by_p = (
            av.groupby("party_name")["votes"].sum().reset_index()
            .rename(columns={"party_name": "Party", "votes": "Candidate_Votes"})
        )
        pty_by_p = (
            apv.groupby("entity_name")["votes"].sum().reset_index()
            .rename(columns={"entity_name": "Party", "votes": "Party_List_Votes"})
        )

    split = (
        cand_by_p.merge(pty_by_p, on="Party", how="outer").fillna(0)
        .assign(Vote_Gap=lambda d: d["Candidate_Votes"] - d["Party_List_Votes"],
                _total=lambda d: d["Candidate_Votes"] + d["Party_List_Votes"])
        .sort_values("_total", ascending=False).head(10).drop(columns="_total")
    )

    if split.empty:
        empty_state("No split-ticket data", "The selected filters returned no overlapping candidate and party-list data.")
    else:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            name="Constituency", y=split["Party"], x=split["Candidate_Votes"],
            orientation="h", marker_color=ORANGE,
        ))
        fig.add_trace(go.Bar(
            name="Party-list", y=split["Party"], x=split["Party_List_Votes"],
            orientation="h", marker_color=LIGHT_OG,
        ))
        fig.update_layout(
            barmode="group", title="Split-Ticket Voting — Top 10 Parties",
            plot_bgcolor=CHART_BG, paper_bgcolor=CHART_BG,
            font_color=WHITE, title_font_color=WHITE,
            xaxis=dict(gridcolor=GRID, zeroline=False, title="Votes", automargin=True),
            yaxis=dict(gridcolor=GRID, categoryorder="total ascending", zeroline=False, title="", automargin=True),
            legend=dict(font=dict(color=WHITE), x=0.99, xanchor="right", y=0.98, yanchor="top"),
            height=430,
            margin=dict(l=8, r=18, t=46, b=36),
            hoverlabel=dict(bgcolor=PANEL_BG, font_color=WHITE, bordercolor=BORDER),
        )
        st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)

        st.caption(
            "Positive gap = candidate outperformed party · Negative gap = party outperformed candidate"
        )
        st.dataframe(
            split[["Party", "Candidate_Votes", "Party_List_Votes", "Vote_Gap"]],
            width="stretch", hide_index=True,
        )


# ─────────────────────────── TAB 5 · DEMOGRAPHICS ─────────────────────────────
with tab5:
    if ver == "V3":
        st.info("Demographics analysis requires station-level data (V1 / V2 / V4 only).")
    else:
        st.subheader("Voting Preference by Station Size")
        st.caption("Stations grouped into Small / Medium / Large by eligible voter count")

        if len(f_st) < 3 or apv.empty:
            empty_state(
                "Not enough station data",
                "Station-size comparison needs at least three filtered stations with party-list votes.",
            )
        else:
            sized = f_st.copy()
            sized["station_size"] = pd.qcut(
                sized["eligible_voters"].rank(method="first"),
                q=3, labels=["Small", "Medium", "Large"],
            )
            top5 = (
                apv.groupby("entity_name")["votes"].sum()
                .nlargest(5).index.tolist()
            )
            merged = (
                apv[apv["entity_name"].isin(top5)]
                .merge(sized[["station_code", "station_size"]], on="station_code", how="inner")
            )
            pivot = (
                merged.groupby(["station_size", "entity_name"], observed=True)["votes"]
                .sum().reset_index()
            )

            if pivot.empty:
                empty_state(
                    "No station-size vote rows",
                    "The filtered stations do not have party-list vote rows to compare.",
                )
            else:
                fig = px.bar(
                    pivot, x="station_size", y="votes", color="entity_name",
                    barmode="group", title="Top 5 Parties by Station Size",
                    color_discrete_sequence=PALETTE,
                    category_orders={
                        "station_size": ["Small", "Medium", "Large"],
                        "entity_name": top5
                    },
                )

                fig.update_layout(
                    plot_bgcolor=CHART_BG, paper_bgcolor=CHART_BG,
                    font_color=WHITE, title_font_color=WHITE,
                    title=dict(text="Top 5 Parties by Station Size", y=0.96, yanchor="top"),
                    xaxis=dict(
                        gridcolor=GRID,
                        zeroline=False,
                        title="",
                        automargin=True,
                    ),
                    yaxis=dict(gridcolor=GRID, zeroline=False, title="", automargin=True),
                    legend=dict(
                        title_text="Party",
                        x=0.99,
                        xanchor="right",
                        y=0.93,
                        yanchor="top",
                        font=dict(color=WHITE, size=11),
                        title_font_color=WHITE,
                    ),
                    height=460,
                    margin=dict(l=28, r=70, t=76, b=58),
                    hoverlabel=dict(bgcolor=PANEL_BG, font_color=WHITE, bordercolor=BORDER),
                )
                st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)


# ─────────────────────────── TAB 6 · COMPARE VERSIONS ─────────────────────────
with tab6:
    st.subheader("V1 vs V2 vs V3 vs V4 — Side by Side")

    # ── Candidate comparison ──
    v1c = f_v.groupby("entity_name")["votes"].sum().reset_index().rename(columns={"votes": "V1"})
    v2c = f_v2.groupby("entity_name")["votes"].sum().reset_index().rename(columns={"votes": "V2"})
    v3c = v3_cand[["entity_name", "votes"]].rename(columns={"votes": "V3"})
    v4c = f_v4.groupby("entity_name")["votes"].sum().reset_index().rename(columns={"votes": "V4"})
    comp_c = (
        v1c.merge(v2c, on="entity_name", how="outer")
           .merge(v3c, on="entity_name", how="outer")
           .merge(v4c, on="entity_name", how="outer")
           .sort_values("V3", ascending=False).reset_index(drop=True)
    )

    fig = go.Figure()
    for col, color, name in [
        ("V1", ORANGE,   "V1 OCR"),
        ("V2", LIGHT_OG, "V2 Scale"),
        ("V3", WHITE,    "V3 Truth"),
        ("V4", "#8172B3", "V4 KNN"),
    ]:
        fig.add_trace(go.Bar(
            name=name, y=comp_c["entity_name"], x=comp_c[col],
            orientation="h", marker_color=color,
        ))
    fig.update_layout(
        barmode="group", title="Constituency Votes — All Versions",
        plot_bgcolor=CHART_BG, paper_bgcolor=CHART_BG,
        font_color=WHITE, title_font_color=WHITE,
        xaxis=dict(gridcolor=GRID, zeroline=False, title="Votes", automargin=True),
        yaxis=dict(gridcolor=GRID, categoryorder="total ascending", zeroline=False, title="", automargin=True),
        legend=dict(font=dict(color=WHITE), x=0.99, xanchor="right", y=0.98, yanchor="top"),
        height=420, margin=dict(l=8, r=18, t=46, b=36),
        hoverlabel=dict(bgcolor=PANEL_BG, font_color=WHITE, bordercolor=BORDER),
    )
    st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)

    # ── Party comparison ──
    v1p = f_pv.groupby("entity_name")["votes"].sum().reset_index().rename(columns={"votes": "V1"})
    v2p = f_v2p.groupby("entity_name")["votes"].sum().reset_index().rename(columns={"votes": "V2"})
    v3p = v3_party[["entity_name", "votes"]].rename(columns={"votes": "V3"})
    v4p = f_v4p.groupby("entity_name")["votes"].sum().reset_index().rename(columns={"votes": "V4"})
    comp_p = (
        v1p.merge(v2p, on="entity_name", how="outer")
           .merge(v3p, on="entity_name", how="outer")
           .merge(v4p, on="entity_name", how="outer")
           .sort_values("V3", ascending=False).head(10).reset_index(drop=True)
    )

    fig2 = go.Figure()
    for col, color, name in [
        ("V1", ORANGE,   "V1 OCR"),
        ("V2", LIGHT_OG, "V2 Scale"),
        ("V3", WHITE,    "V3 Truth"),
        ("V4", "#8172B3", "V4 KNN"),
    ]:
        fig2.add_trace(go.Bar(
            name=name, y=comp_p["entity_name"], x=comp_p[col],
            orientation="h", marker_color=color,
        ))
    fig2.update_layout(
        barmode="group", title="Party-List Votes — Top 10, All Versions",
        plot_bgcolor=CHART_BG, paper_bgcolor=CHART_BG,
        font_color=WHITE, title_font_color=WHITE,
        xaxis=dict(gridcolor=GRID, zeroline=False, title="Votes", automargin=True),
        yaxis=dict(gridcolor=GRID, categoryorder="total ascending", zeroline=False, title="", automargin=True),
        legend=dict(font=dict(color=WHITE), x=0.99, xanchor="right", y=0.98, yanchor="top"),
        height=460, margin=dict(l=8, r=18, t=46, b=36),
        hoverlabel=dict(bgcolor=PANEL_BG, font_color=WHITE, bordercolor=BORDER),
    )
    st.plotly_chart(fig2, width="stretch", config=PLOTLY_CONFIG)

    # ── Accuracy table ──
    st.subheader("OCR Accuracy vs Ground Truth")
    acc = comp_c.copy()
    acc["V1 acc %"] = (acc["V1"] / acc["V3"] * 100).round(1)
    acc["V2 acc %"] = (acc["V2"] / acc["V3"] * 100).round(1)
    acc["V4 acc %"] = (acc["V4"] / acc["V3"] * 100).round(1)
    st.dataframe(
        acc[["entity_name", "V1", "V2", "V3", "V4", "V1 acc %", "V2 acc %", "V4 acc %"]]
        .rename(columns={"entity_name": "Candidate"}),
        width="stretch", hide_index=True,
    )

    col_a, col_b = st.columns(2)
    col_a.metric("Constituency OCR Coverage",
                 f"{f_v['votes'].sum() / ref_cand_total * 100:.1f}%")
    col_b.metric("Party-List OCR Coverage",
                 f"{f_pv['votes'].sum() / ref_party_total * 100:.1f}%")


# ─────────────────────────── TAB 7 · DATA QUALITY ─────────────────────────────
with tab7:
    flagged = station_df[station_df["validation_flags"].notna()].copy()
    st.markdown(
        f"<p style='color:{LGRAY}'>{len(flagged)} stations have validation flags</p>",
        unsafe_allow_html=True,
    )

    flag_values = split_validation_flags(flagged["validation_flags"])
    flag_names = flag_values.str.split(":").str[0] if not flag_values.empty else pd.Series(dtype="string")
    flag_counts = flag_names.value_counts().rename_axis("Flag").reset_index(name="Count")

    if flag_counts.empty:
        empty_state("No validation flags", "No station validation flags are present in the current dataset.")
    else:
        fig = styled_bar(flag_counts.head(12), "Count", "Flag",
                         "Top Validation Flags", color=DARK_OG, height=380)
        st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)

    st.dataframe(
        flagged[["station_code", "district", "subdistrict", "validation_flags"]],
        width="stretch", hide_index=True,
    )

# ─────────────────────────── TAB 8 · MISSING DATA ─────────────────────────────
with tab8:
    st.subheader("Missing Polling Stations")
    st.markdown(f"<p style='color:{LGRAY}'>Stations found in reference data but missing from our parsed datasets.</p>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"**Constituency Missing Stations** ({len(missing_st_df)})")
        if not missing_st_df.empty:
            st.dataframe(missing_st_df, width="stretch", hide_index=True)
        else:
            st.success("No missing constituency stations!")
            
    with col2:
        st.markdown(f"**Party-List Missing Stations** ({len(missing_pst_df)})")
        if not missing_pst_df.empty:
            st.dataframe(missing_pst_df, width="stretch", hide_index=True)
        else:
            st.success("No missing party-list stations!")
