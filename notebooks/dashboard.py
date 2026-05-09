import json
import html
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
PARTY_FALLBACK_PALETTE = [
    "#B8C0CC", "#8BC6D9", "#D9B86C", "#C7A0D8", "#82B29A",
    "#D9988F", "#A8B8E8", "#E7C6A2", "#9BC8B8", "#C9D36A",
]
PARTY_COLORS = {
    "ไทยทรัพย์ทวี": "#0A62D8",
    "เพื่อชาติไทย": "#B71C1C",
    "ใหม่": "#4C88D9",
    "มิติใหม่": "#D9A08D",
    "รวมใจไทย": "#C9D36A",
    "รวมไทยสร้างชาติ": "#1F3A8A",
    "พลวัต": "#6A3D9A",
    "ประชาธิปไตยใหม่": "#1E88E5",
    "เพื่อไทย": "#D71920",
    "ทางเลือกใหม่": "#3157A4",
    "เศรษฐกิจ": "#F5C645",
    "เสรีรวมไทย": "#C7A23A",
    "รวมพลังประชาชน": "#2E8B57",
    "ท้องที่ไทย": "#0B6B4A",
    "อนาคตไทย": "#F36C21",
    "พลังเพื่อไทย": "#D71920",
    "ไทยชนะ": "#21409A",
    "พลังสังคมใหม่": "#2E7D32",
    "สังคมประชาธิปไตยไทย": "#C62828",
    "ฟิวชัน": "#7E57C2",
    "ไทรวมพลัง": "#1F4E79",
    "ก้าวอิสระ": "#AA2E25",
    "ปวงชนไทย": "#2E5AAC",
    "วิชชั่นใหม่": "#8E44AD",
    "เพื่อชีวิตใหม่": "#43A047",
    "คลองไทย": "#00A6D6",
    "ประชาธิปัตย์": "#5DB7E5",
    "ไทยก้าวหน้า": "#2A9D8F",
    "ไทยภักดี": "#0D4F3D",
    "แรงงานสร้างชาติ": "#C62828",
    "ประชากรไทย": "#7EC8E3",
    "ครูไทยเพื่อประชาชน": "#F0A9B8",
    "ประชาชาติ": "#7A3E98",
    "สร้างอนาคตไทย": "#C9A227",
    "รักชาติ": "#9FA8DA",
    "ไทยพร้อม": "#D99058",
    "ภูมิใจไทย": "#174EA6",
    "พลังธรรมใหม่": "#009688",
    "กรีน": "#2E7D32",
    "ไทยธรรม": "#8D6E63",
    "แผ่นดินธรรม": "#A8863B",
    "กล้าธรรม": "#226D7A",
    "พลังประชารัฐ": "#1F4991",
    "โอกาสใหม่": "#F5D76E",
    "เป็นธรรม": "#F36C21",
    "ประชาชน": "#F05A28",
    "ประชาไทย": "#2A66B2",
    "ไทยสร้างไทย": "#7B3FB2",
    "ไทยก้าวใหม่": "#2A9D8F",
    "ประชาอาสาชาติ": "#8BC6D9",
    "พร้อม": "#A8B8E8",
    "เครือข่ายชาวนาแห่งประเทศไทย": "#7CB342",
    "ไทยพิทักษ์ธรรม": "#B38867",
    "ความหวังใหม่": "#2E8B57",
    "ไทยรวมไทย": "#3949AB",
    "เพื่อบ้านเมือง": "#C46A45",
    "พลังไทยรักชาติ": "#F2C94C",
}

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
      height: 0 !important;
      min-height: 0 !important;
      pointer-events: none !important;
      overflow: visible;
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
  [data-testid="stSelectbox"] div[data-baseweb="select"],
  [data-testid="stSelectbox"] div[data-baseweb="select"] *,
  div[data-baseweb="select"],
  div[data-baseweb="select"] *,
  div[data-baseweb="popover"] [role="listbox"],
  div[data-baseweb="popover"] [role="option"],
  div[data-baseweb="popover"] [role="option"] * {{
      cursor: pointer !important;
  }}
  [data-testid="stSelectbox"] label,
  [data-testid="stSelectbox"] label * {{
      cursor: default !important;
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
DISTRICT_GEOJSON = EXTERNAL / "chaiyaphum_2_districts.geojson"
SUBDISTRICT_GEOJSON = EXTERNAL / "chaiyaphum_2_subdistricts.geojson"
ELECTION66 = EXTERNAL / "Election66"
ELECTION66_PROCESSED = ELECTION66 / "processed"

ELECTION66_PARTY_REMAP = {
    "ก้าวไกล": "ประชาชน",
}
ELECTION66_PARTY_DISPLAY = {
    "ประชาชน": "ก้าวไกล / ประชาชน",
}


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


@st.cache_data
def load_election66_data():
    def empty():
        return pd.DataFrame()

    candidates = empty()
    candidate_path = ELECTION66_PROCESSED / "chaiyaphum_2_candidates_2566.csv"
    raw_candidate_path = ELECTION66 / "candidate66.csv"
    if candidate_path.exists():
        candidates = pd.read_csv(candidate_path, encoding="utf-8-sig")
    elif raw_candidate_path.exists():
        candidates = (
            pd.read_csv(raw_candidate_path, encoding="utf-8-sig")
            .assign(province="ชัยภูมิ", constituency_no=2)
            .rename(columns={"vote_count": "official_candidate_total"})
        )

    party_totals = empty()
    party_path = ELECTION66_PROCESSED / "chaiyaphum_2_party_totals_2566.csv"
    raw_score_path = ELECTION66 / "election_scores_2566.csv"
    if party_path.exists():
        party_totals = pd.read_csv(party_path, encoding="utf-8-sig")
    elif raw_score_path.exists():
        raw_scores = pd.read_csv(raw_score_path, encoding="utf-8-sig")
        raw_scores = raw_scores[
            raw_scores["province"].eq("ชัยภูมิ") & raw_scores["province_number"].eq(2)
        ]
        summary_names = {"ผู้มีสิทธิ์", "ผู้มาใช้สิทธิ์", "บัตรเสีย", "ไม่เลือกผู้ใด"}
        party_cols = [
            col for col in raw_scores.columns
            if col.startswith("บช_") and col.removeprefix("บช_") not in summary_names
        ]
        party_totals = (
            raw_scores[party_cols]
            .fillna(0)
            .sum()
            .rename_axis("party_name")
            .reset_index(name="votes")
        )
        party_totals["party_name"] = party_totals["party_name"].str.removeprefix("บช_")
        party_totals = party_totals.sort_values("votes", ascending=False, ignore_index=True)

    paths = {
        "area_summary": ELECTION66_PROCESSED / "chaiyaphum_2_area_summary_2566.csv",
        "candidate_area_long": ELECTION66_PROCESSED / "chaiyaphum_2_candidate_votes_area_long_2566.csv",
        "party_area_long": ELECTION66_PROCESSED / "chaiyaphum_2_party_votes_area_long_2566.csv",
    }
    data = {
        "candidates": candidates,
        "party_totals": party_totals,
    }
    for key, path in paths.items():
        data[key] = pd.read_csv(path, encoding="utf-8-sig") if path.exists() else empty()
    return data


try:
    station_df, votes_df, p_station_df, p_votes_df, cand_ref, party_ref, v4_votes_df, v4_party_df, missing_st_df, missing_pst_df = load_data()
except FileNotFoundError as e:
    st.error(f"⚠️ Data files not found: {e}")
    st.stop()

election66 = load_election66_data()

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


def right_side_legend(title_text=None, traceorder=None):
    legend = dict(
        x=1.01,
        xanchor="left",
        y=1,
        yanchor="top",
        bgcolor="rgba(11,13,18,0.78)",
        bordercolor=BORDER,
        borderwidth=1,
        font=dict(color=WHITE, size=11),
    )
    if title_text:
        legend["title_text"] = title_text
        legend["title_font_color"] = WHITE
    if traceorder:
        legend["traceorder"] = traceorder
    return legend


def percent_axis_range(values):
    max_value = pd.to_numeric(pd.Series(values), errors="coerce").max()
    if pd.isna(max_value) or max_value <= 0:
        return [0, 1]
    return [0, min(100, max(5, max_value * 1.12))]


def fallback_party_color(name):
    value = str(name)
    idx = sum(ord(ch) for ch in value) % len(PARTY_FALLBACK_PALETTE)
    return PARTY_FALLBACK_PALETTE[idx]


def party_color(name):
    value = str(name).strip()
    return PARTY_COLORS.get(value, fallback_party_color(value))


def party_colors(names):
    return [party_color(name) for name in names]


def party_color_map(names):
    return {
        str(name): party_color(name)
        for name in pd.Series(names).dropna().unique()
    }


def styled_party_bar(df, x, y, title, height=400):
    fig = styled_bar(df, x, y, title, color=LIGHT_OG, height=height)
    fig.update_traces(marker_color=party_colors(df[y]))
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


def filter_election66_area(df, district, subdistrict):
    filtered = df.copy()
    if filtered.empty:
        return filtered
    if district != "All":
        filtered = filtered[filtered["district"] == district]
    if subdistrict != "All":
        filtered = filtered[filtered["subdistrict"] == subdistrict]
    return filtered


def election66_has_scope(district, subdistrict):
    return district != "All" or subdistrict != "All"


def party_compare_key(series):
    return series.astype(str).str.strip().replace(ELECTION66_PARTY_REMAP)


def party_compare_label(party_name):
    value = str(party_name).strip()
    return ELECTION66_PARTY_DISPLAY.get(value, value)


def current_candidate_totals(av_df, version):
    if version == "V3":
        return (
            v3_cand.rename(columns={
                "entity_name": "candidate_name",
                "party_name": "party_name_2026",
                "votes": "votes_2026",
            })[["candidate_name", "party_name_2026", "votes_2026"]]
        )
    return (
        av_df.groupby(["entity_name", "party_name"], dropna=False)["votes"]
        .sum()
        .reset_index()
        .rename(columns={
            "entity_name": "candidate_name",
            "party_name": "party_name_2026",
            "votes": "votes_2026",
        })
    )


def current_party_totals(apv_df, version):
    if version == "V3":
        current = v3_party.rename(columns={"entity_name": "party_name", "votes": "votes_2026"})
    else:
        current = (
            apv_df.groupby("entity_name")["votes"]
            .sum()
            .reset_index()
            .rename(columns={"entity_name": "party_name", "votes": "votes_2026"})
        )
    current["compare_party"] = party_compare_key(current["party_name"])
    return current.groupby("compare_party", as_index=False)["votes_2026"].sum()


def election66_candidate_totals(data, district, subdistrict):
    scoped = election66_has_scope(district, subdistrict)
    if scoped and not data["candidate_area_long"].empty:
        source = filter_election66_area(data["candidate_area_long"], district, subdistrict)
        if source.empty:
            return pd.DataFrame(columns=["candidate_name", "party_name_2023", "votes_2023"])
        return (
            source.groupby(["candidate_name", "party_name"], as_index=False)["votes"]
            .sum()
            .rename(columns={"party_name": "party_name_2023", "votes": "votes_2023"})
        )

    candidates = data["candidates"]
    if candidates.empty:
        return pd.DataFrame(columns=["candidate_name", "party_name_2023", "votes_2023"])
    return candidates.rename(columns={
        "party_name": "party_name_2023",
        "official_candidate_total": "votes_2023",
    })[["candidate_name", "party_name_2023", "votes_2023"]]


def election66_party_totals(data, district, subdistrict):
    scoped = election66_has_scope(district, subdistrict)
    if scoped and not data["party_area_long"].empty:
        source = filter_election66_area(data["party_area_long"], district, subdistrict)
        if source.empty:
            return pd.DataFrame(columns=["compare_party", "votes_2023"])
        source = source.copy()
        source["compare_party"] = party_compare_key(source["party_name"])
        return source.groupby("compare_party", as_index=False)["votes"].sum().rename(columns={"votes": "votes_2023"})

    parties = data["party_totals"]
    if parties.empty:
        return pd.DataFrame(columns=["compare_party", "votes_2023"])
    parties = parties.copy()
    parties["compare_party"] = party_compare_key(parties["party_name"])
    return parties.groupby("compare_party", as_index=False)["votes"].sum().rename(columns={"votes": "votes_2023"})


def election66_turnout_by_area(data, district, subdistrict):
    area = filter_election66_area(data["area_summary"], district, subdistrict)
    if area.empty:
        return pd.DataFrame(columns=["district", "subdistrict", "turnout_2023"])
    return area.rename(columns={"turnout_pct": "turnout_2023"})[
        ["district", "subdistrict", "turnout_2023", "station_count"]
    ]


@st.cache_data
def load_geojson(path: str):
    with open(path, encoding="utf-8") as file:
        return json.load(file)


def geojson_area_frame(geojson, area_level):
    rows = []
    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        district = props.get("district") or props.get("NAME2") or props.get("AMPHOE_T")
        subdistrict = props.get("subdistrict") or props.get("NAME3")
        if area_level == "District":
            area_key = props.get("area_key") or district
            area_name = district
        else:
            area_key = props.get("area_key") or f"{district}||{subdistrict}"
            area_name = subdistrict
        rows.append({
            "area_key": area_key,
            "area_name": area_name,
            "district": district,
            "subdistrict": subdistrict,
        })
    return pd.DataFrame(rows)


def filter_geojson_features(geojson, area_keys):
    key_set = set(pd.Series(area_keys).dropna().astype(str))
    return {
        **{k: v for k, v in geojson.items() if k != "features"},
        "features": [
            feature
            for feature in geojson.get("features", [])
            if str(feature.get("properties", {}).get("area_key")) in key_set
        ],
    }


def add_area_key(df, area_level):
    keyed = df.copy()
    if keyed.empty:
        keyed["area_key"] = pd.Series(dtype="string")
        return keyed
    if area_level == "District":
        keyed["area_key"] = keyed["district"].astype(str)
    else:
        keyed["area_key"] = keyed["district"].astype(str) + "||" + keyed["subdistrict"].astype(str)
    return keyed


def aggregate_station_map(stations, area_level):
    stations = add_area_key(stations, area_level)
    if stations.empty:
        return pd.DataFrame(columns=[
            "area_key", "stations", "eligible_voters", "voters_present", "turnout_pct"
        ])
    summary = (
        stations.groupby("area_key")
        .agg(
            stations=("station_code", "nunique"),
            eligible_voters=("eligible_voters", "sum"),
            voters_present=("voters_present", "sum"),
        )
        .reset_index()
    )
    summary["turnout_pct"] = np.where(
        summary["eligible_voters"].gt(0),
        summary["voters_present"] / summary["eligible_voters"] * 100,
        np.nan,
    )
    return summary


def aggregate_vote_map(votes, area_level, vote_type):
    columns = [
        "area_key", "total_votes", "winner", "winner_party", "winner_votes",
        "runner_up", "runner_up_votes", "margin_votes", "margin_pct",
    ]
    votes = add_area_key(votes, area_level)
    if votes.empty:
        return pd.DataFrame(columns=columns)

    if vote_type == "Constituency":
        grouped = (
            votes.groupby(["area_key", "entity_name", "party_name"], dropna=False)["votes"]
            .sum()
            .reset_index()
        )
        grouped["party_for_color"] = grouped["party_name"].fillna(grouped["entity_name"])
        grouped["choice_label"] = np.where(
            grouped["party_name"].notna(),
            grouped["entity_name"].astype(str) + " (" + grouped["party_name"].astype(str) + ")",
            grouped["entity_name"].astype(str),
        )
    else:
        grouped = (
            votes.groupby(["area_key", "entity_name"], dropna=False)["votes"]
            .sum()
            .reset_index()
        )
        grouped["party_for_color"] = grouped["entity_name"]
        grouped["choice_label"] = grouped["entity_name"].astype(str)

    grouped = grouped.sort_values(["area_key", "votes"], ascending=[True, False])
    top_two = grouped.groupby("area_key", as_index=False).head(2).copy()
    top_two["rank"] = top_two.groupby("area_key").cumcount() + 1

    winners = (
        top_two[top_two["rank"] == 1]
        .rename(columns={
            "choice_label": "winner",
            "party_for_color": "winner_party",
            "votes": "winner_votes",
        })
        [["area_key", "winner", "winner_party", "winner_votes"]]
    )
    runners = (
        top_two[top_two["rank"] == 2]
        .rename(columns={
            "choice_label": "runner_up",
            "votes": "runner_up_votes",
        })
        [["area_key", "runner_up", "runner_up_votes"]]
    )
    totals = grouped.groupby("area_key")["votes"].sum().reset_index(name="total_votes")

    result = (
        totals.merge(winners, on="area_key", how="left")
        .merge(runners, on="area_key", how="left")
    )
    result["runner_up"] = result["runner_up"].fillna("No runner-up")
    result["runner_up_votes"] = result["runner_up_votes"].fillna(0)
    result["margin_votes"] = result["winner_votes"] - result["runner_up_votes"]
    result["margin_pct"] = np.where(
        result["total_votes"].gt(0),
        result["margin_votes"] / result["total_votes"] * 100,
        np.nan,
    )
    return result[columns]


def prepare_map_data(stations, votes, geojson, area_level, district, subdistrict, vote_type):
    geo_df = geojson_area_frame(geojson, area_level)
    if district != "All":
        geo_df = geo_df[geo_df["district"] == district]
    if area_level == "Sub-district" and subdistrict != "All":
        geo_df = geo_df[geo_df["subdistrict"] == subdistrict]

    station_summary = aggregate_station_map(stations, area_level)
    vote_summary = aggregate_vote_map(votes, area_level, vote_type)

    map_df = (
        geo_df.merge(station_summary, on="area_key", how="left")
        .merge(vote_summary, on="area_key", how="left")
    )
    map_df["stations"] = map_df["stations"].fillna(0).astype(int)
    for col in ["eligible_voters", "voters_present", "total_votes", "winner_votes", "runner_up_votes", "margin_votes"]:
        map_df[col] = map_df[col].fillna(0)
    map_df["winner"] = map_df["winner"].fillna("No vote rows in current scope")
    map_df["winner_party"] = map_df["winner_party"].fillna("No data")
    map_df["runner_up"] = map_df["runner_up"].fillna("No vote rows")
    map_df["winner_party_display"] = map_df["winner_party"]
    map_df["has_votes"] = map_df["total_votes"].gt(0)
    return map_df


def build_map_figure(map_df, geojson, area_level, metric):
    visible_geojson = filter_geojson_features(geojson, map_df["area_key"])
    center = {"lat": 15.74, "lon": 101.92}
    zoom = 8.0 if area_level == "District" else 8.4
    base_hover = {
        "area_key": False,
        "winner_party": False,
        "winner_party_display": False,
        "has_votes": False,
        "district": True,
        "subdistrict": area_level == "Sub-district",
        "stations": ":,.0f",
        "eligible_voters": ":,.0f",
        "voters_present": ":,.0f",
        "turnout_pct": ":.2f",
        "winner": True,
        "winner_votes": ":,.0f",
        "runner_up": True,
        "runner_up_votes": ":,.0f",
        "margin_votes": ":,.0f",
        "margin_pct": ":.2f",
        "total_votes": ":,.0f",
    }

    if metric == "Winner":
        color_map = {
            name: party_color(name)
            for name in map_df["winner_party_display"].dropna().unique()
            if name != "No data"
        }
        color_map["No data"] = MUTED
        fig = px.choropleth_map(
            map_df,
            geojson=visible_geojson,
            locations="area_key",
            featureidkey="properties.area_key",
            color="winner_party_display",
            color_discrete_map=color_map,
            hover_name="area_name",
            hover_data=base_hover,
            map_style="dark",
            center=center,
            zoom=zoom,
            opacity=0.82,
            title=f"Election Winner by {area_level}",
        )
    else:
        metric_map = {
            "Turnout %": ("turnout_pct", "Turnout %"),
            "Victory margin %": ("margin_pct", "Victory margin %"),
            "Total votes": ("total_votes", "Total votes"),
        }
        color_col, title = metric_map[metric]
        valid = map_df[map_df["has_votes"] & map_df[color_col].notna()].copy()
        if valid.empty:
            fig = go.Figure()
        else:
            fig = px.choropleth_map(
                valid,
                geojson=filter_geojson_features(visible_geojson, valid["area_key"]),
                locations="area_key",
                featureidkey="properties.area_key",
                color=color_col,
                color_continuous_scale=[PANEL_BG, LIGHT_OG, ORANGE],
                hover_name="area_name",
                hover_data=base_hover,
                map_style="dark",
                center=center,
                zoom=zoom,
                opacity=0.84,
                title=f"{title} by {area_level}",
            )
            fig.update_layout(coloraxis_colorbar=dict(title=title))
        no_data = map_df[~map_df["has_votes"]]
        if not no_data.empty:
            fig.add_trace(go.Choroplethmap(
                geojson=filter_geojson_features(visible_geojson, no_data["area_key"]),
                locations=no_data["area_key"],
                featureidkey="properties.area_key",
                z=[0] * len(no_data),
                colorscale=[[0, MUTED], [1, MUTED]],
                showscale=False,
                marker_opacity=0.42,
                marker_line_color="rgba(255,255,255,0.38)",
                marker_line_width=1,
                text=no_data["area_name"],
                hovertemplate="<b>%{text}</b><br>No vote rows in current scope<extra></extra>",
                name="No data",
            ))

    fig.update_traces(marker_line_color="rgba(255,255,255,0.42)", marker_line_width=1)
    fig.update_layout(
        paper_bgcolor=CHART_BG,
        plot_bgcolor=CHART_BG,
        font_color=WHITE,
        title_font_color=WHITE,
        height=620,
        margin=dict(l=8, r=8, t=54, b=8),
        hoverlabel=dict(bgcolor=PANEL_BG, font_color=WHITE, bordercolor=BORDER),
        legend=dict(
            title_text="Winner party",
            font=dict(color=WHITE, size=11),
            title_font_color=WHITE,
            bgcolor="rgba(11,13,18,0.72)",
            bordercolor=BORDER,
            borderwidth=1,
            x=0.01,
            y=0.99,
        ),
        map=dict(
            style="dark",
            center=center,
            zoom=zoom,
        ),
    )
    return fig


def robust_zscore(series):
    values = pd.to_numeric(series, errors="coerce")
    median = values.median()
    mad = (values - median).abs().median()
    if pd.isna(median):
        return pd.Series(0.0, index=series.index)
    if pd.isna(mad) or mad == 0:
        std = values.std(ddof=0)
        if pd.isna(std) or std == 0:
            return pd.Series(0.0, index=series.index)
        return ((values - median) / std).fillna(0.0)
    return (0.6745 * (values - median) / mad).fillna(0.0)


def station_analysis_base(stations):
    if stations.empty:
        return pd.DataFrame(columns=[
            "station_code", "station_no", "district", "subdistrict",
            "eligible_voters", "voters_present", "ballots_used",
            "ballots_spoiled", "ballots_no_vote", "turnout_pct",
            "spoiled_pct", "no_vote_pct", "validation_flags",
        ])

    base = (
        stations.groupby("station_code", dropna=False)
        .agg(
            station_no=("station_no", "first"),
            district=("district", "first"),
            subdistrict=("subdistrict", "first"),
            eligible_voters=("eligible_voters", "sum"),
            voters_present=("voters_present", "sum"),
            ballots_used=("ballots_used", "sum"),
            ballots_spoiled=("ballots_spoiled", "sum"),
            ballots_no_vote=("ballots_no_vote", "sum"),
            validation_flags=("validation_flags", lambda s: "; ".join(sorted(set(s.dropna().astype(str))))),
        )
        .reset_index()
    )
    base["turnout_pct"] = np.where(
        base["eligible_voters"].gt(0),
        base["voters_present"] / base["eligible_voters"] * 100,
        np.nan,
    )
    base["spoiled_pct"] = np.where(
        base["ballots_used"].gt(0),
        base["ballots_spoiled"] / base["ballots_used"] * 100,
        np.nan,
    )
    base["no_vote_pct"] = np.where(
        base["ballots_used"].gt(0),
        base["ballots_no_vote"] / base["ballots_used"] * 100,
        np.nan,
    )
    return base


def station_candidate_strength(candidate_votes):
    columns = [
        "station_code", "winner_candidate", "winner_party", "winner_votes",
        "runner_up_candidate", "runner_up_votes", "candidate_total_votes",
        "winner_share_pct", "margin_pct",
    ]
    if candidate_votes.empty:
        return pd.DataFrame(columns=columns)

    grouped = (
        candidate_votes.groupby(["station_code", "entity_name", "party_name"], dropna=False)["votes"]
        .sum()
        .reset_index()
        .sort_values(["station_code", "votes"], ascending=[True, False])
    )
    grouped["rank"] = grouped.groupby("station_code").cumcount() + 1
    totals = grouped.groupby("station_code")["votes"].sum().reset_index(name="candidate_total_votes")
    winners = (
        grouped[grouped["rank"] == 1]
        .rename(columns={
            "entity_name": "winner_candidate",
            "party_name": "winner_party",
            "votes": "winner_votes",
        })[["station_code", "winner_candidate", "winner_party", "winner_votes"]]
    )
    runners = (
        grouped[grouped["rank"] == 2]
        .rename(columns={
            "entity_name": "runner_up_candidate",
            "votes": "runner_up_votes",
        })[["station_code", "runner_up_candidate", "runner_up_votes"]]
    )
    result = totals.merge(winners, on="station_code", how="left").merge(runners, on="station_code", how="left")
    result["runner_up_candidate"] = result["runner_up_candidate"].fillna("No runner-up")
    result["runner_up_votes"] = result["runner_up_votes"].fillna(0)
    result["winner_share_pct"] = np.where(
        result["candidate_total_votes"].gt(0),
        result["winner_votes"] / result["candidate_total_votes"] * 100,
        np.nan,
    )
    result["margin_pct"] = np.where(
        result["candidate_total_votes"].gt(0),
        (result["winner_votes"] - result["runner_up_votes"]) / result["candidate_total_votes"] * 100,
        np.nan,
    )
    return result[columns]


def station_split_gap(candidate_votes, party_votes):
    columns = [
        "station_code", "split_party", "candidate_share_pct",
        "party_list_share_pct", "split_gap_pp", "split_abs_gap_pp",
    ]
    if candidate_votes.empty or party_votes.empty:
        return pd.DataFrame(columns=columns)

    cand = (
        candidate_votes.dropna(subset=["party_name"])
        .groupby(["station_code", "party_name"], as_index=False)["votes"]
        .sum()
        .rename(columns={"votes": "candidate_votes"})
    )
    party = (
        party_votes.groupby(["station_code", "entity_name"], as_index=False)["votes"]
        .sum()
        .rename(columns={"entity_name": "party_name", "votes": "party_list_votes"})
    )
    if cand.empty or party.empty:
        return pd.DataFrame(columns=columns)

    cand_total = cand.groupby("station_code")["candidate_votes"].sum().reset_index(name="candidate_total")
    party_total = party.groupby("station_code")["party_list_votes"].sum().reset_index(name="party_total")
    split = (
        cand.merge(party, on=["station_code", "party_name"], how="outer")
        .fillna({"candidate_votes": 0, "party_list_votes": 0})
        .merge(cand_total, on="station_code", how="left")
        .merge(party_total, on="station_code", how="left")
    )
    split["candidate_share_pct"] = np.where(
        split["candidate_total"].gt(0),
        split["candidate_votes"] / split["candidate_total"] * 100,
        0.0,
    )
    split["party_list_share_pct"] = np.where(
        split["party_total"].gt(0),
        split["party_list_votes"] / split["party_total"] * 100,
        0.0,
    )
    split["split_gap_pp"] = split["candidate_share_pct"] - split["party_list_share_pct"]
    split["split_abs_gap_pp"] = split["split_gap_pp"].abs()
    return (
        split.sort_values(["station_code", "split_abs_gap_pp"], ascending=[True, False])
        .groupby("station_code", as_index=False)
        .head(1)
        .rename(columns={"party_name": "split_party"})[columns]
    )


def build_station_anomaly(stations, candidate_votes, party_votes):
    base = station_analysis_base(stations)
    if base.empty:
        return base

    result = (
        base.merge(station_candidate_strength(candidate_votes), on="station_code", how="left")
        .merge(station_split_gap(candidate_votes, party_votes), on="station_code", how="left")
    )
    for col in ["winner_share_pct", "margin_pct", "split_abs_gap_pp"]:
        result[col] = result[col].fillna(0.0)

    feature_labels = {
        "turnout_pct": "turnout",
        "spoiled_pct": "spoiled ballots",
        "no_vote_pct": "no-vote",
        "winner_share_pct": "winner share",
        "margin_pct": "victory margin",
        "split_abs_gap_pp": "split-ticket gap",
    }
    weights = {
        "turnout_pct": 0.20,
        "spoiled_pct": 0.13,
        "no_vote_pct": 0.10,
        "winner_share_pct": 0.18,
        "margin_pct": 0.17,
        "split_abs_gap_pp": 0.22,
    }

    weighted_score = pd.Series(0.0, index=result.index)
    for col, weight in weights.items():
        z_col = f"z_{col}"
        result[z_col] = robust_zscore(result[col]).abs().clip(upper=4)
        weighted_score += result[z_col] * weight

    result["review_score"] = (weighted_score / 4 * 100).clip(0, 100)
    result["review_level"] = pd.cut(
        result["review_score"],
        bins=[-0.01, 35, 55, 75, 100],
        labels=["Normal", "Watch", "Review", "High"],
    ).astype(str)

    def reason(row):
        reasons = [
            label
            for col, label in feature_labels.items()
            if row.get(f"z_{col}", 0) >= 2
        ]
        if reasons:
            return ", ".join(reasons)
        return "overall pattern"

    result["review_reason"] = result.apply(reason, axis=1)
    return result.sort_values("review_score", ascending=False).reset_index(drop=True)


def aggregate_anomaly_area(anomaly_df, area_level):
    if anomaly_df.empty:
        return pd.DataFrame(columns=[
            "area_key", "district", "subdistrict", "stations",
            "flagged_stations", "max_review_score", "avg_review_score",
        ])
    keyed = add_area_key(anomaly_df, area_level)
    grouped = (
        keyed.groupby("area_key")
        .agg(
            district=("district", "first"),
            subdistrict=("subdistrict", "first"),
            stations=("station_code", "nunique"),
            flagged_stations=("review_score", lambda s: int((s >= 55).sum())),
            max_review_score=("review_score", "max"),
            avg_review_score=("review_score", "mean"),
        )
        .reset_index()
    )
    return grouped


def build_area_metric_map(area_df, geojson, area_level, color_col, title, colorbar_title, color_scale=None, midpoint=None):
    geo_df = geojson_area_frame(geojson, area_level)
    area_values = area_df.copy()
    for col in ["district", "subdistrict", "area_name"]:
        if col in area_values.columns:
            area_values = area_values.drop(columns=col)
    map_df = geo_df.merge(area_values, on="area_key", how="left")
    for col in ["stations", "flagged_stations"]:
        if col not in map_df.columns:
            map_df[col] = 0
    for col in [color_col, "stations", "flagged_stations", "max_review_score", "avg_review_score"]:
        if col in map_df.columns:
            map_df[col] = pd.to_numeric(map_df[col], errors="coerce").fillna(0)
    if color_scale is None:
        color_scale = [PANEL_BG, LIGHT_OG, ORANGE]

    fig = px.choropleth_map(
        map_df,
        geojson=filter_geojson_features(geojson, map_df["area_key"]),
        locations="area_key",
        featureidkey="properties.area_key",
        color=color_col,
        color_continuous_scale=color_scale,
        color_continuous_midpoint=midpoint,
        hover_name="area_name",
        hover_data={
            "area_key": False,
            "district": True,
            "subdistrict": area_level == "Sub-district",
            color_col: ":.2f",
            "stations": ":,.0f" if "stations" in map_df.columns else False,
            "flagged_stations": ":,.0f" if "flagged_stations" in map_df.columns else False,
        },
        map_style="dark",
        center={"lat": 15.74, "lon": 101.92},
        zoom=8.0 if area_level == "District" else 8.4,
        opacity=0.84,
        title=title,
    )
    fig.update_traces(marker_line_color="rgba(255,255,255,0.42)", marker_line_width=1)
    fig.update_layout(
        paper_bgcolor=CHART_BG,
        plot_bgcolor=CHART_BG,
        font_color=WHITE,
        title_font_color=WHITE,
        height=560,
        margin=dict(l=8, r=8, t=54, b=8),
        coloraxis_colorbar=dict(title=colorbar_title),
        hoverlabel=dict(bgcolor=PANEL_BG, font_color=WHITE, bordercolor=BORDER),
        map=dict(style="dark", center={"lat": 15.74, "lon": 101.92}, zoom=8.0 if area_level == "District" else 8.4),
    )
    return fig, map_df


def prepare_party_area_index(candidate_votes, party_votes, area_level):
    columns = [
        "area_key", "district", "subdistrict", "party_name",
        "candidate_votes", "party_list_votes", "candidate_share_pct",
        "party_list_share_pct", "personal_vote_index_pp",
        "abs_personal_vote_index_pp", "gap_votes",
    ]
    if candidate_votes.empty or party_votes.empty:
        return pd.DataFrame(columns=columns)

    cand = add_area_key(candidate_votes.dropna(subset=["party_name"]), area_level)
    plist = add_area_key(party_votes, area_level)
    cand = (
        cand.groupby(["area_key", "district", "subdistrict", "party_name"], as_index=False)["votes"]
        .sum()
        .rename(columns={"votes": "candidate_votes"})
    )
    plist = (
        plist.groupby(["area_key", "district", "subdistrict", "entity_name"], as_index=False)["votes"]
        .sum()
        .rename(columns={"entity_name": "party_name", "votes": "party_list_votes"})
    )
    if cand.empty and plist.empty:
        return pd.DataFrame(columns=columns)

    result = (
        cand.merge(plist, on=["area_key", "district", "subdistrict", "party_name"], how="outer")
        .fillna({"candidate_votes": 0, "party_list_votes": 0})
    )
    result["candidate_total"] = result.groupby("area_key")["candidate_votes"].transform("sum")
    result["party_list_total"] = result.groupby("area_key")["party_list_votes"].transform("sum")
    result["candidate_share_pct"] = np.where(
        result["candidate_total"].gt(0),
        result["candidate_votes"] / result["candidate_total"] * 100,
        0.0,
    )
    result["party_list_share_pct"] = np.where(
        result["party_list_total"].gt(0),
        result["party_list_votes"] / result["party_list_total"] * 100,
        0.0,
    )
    result["personal_vote_index_pp"] = result["candidate_share_pct"] - result["party_list_share_pct"]
    result["abs_personal_vote_index_pp"] = result["personal_vote_index_pp"].abs()
    result["gap_votes"] = result["candidate_votes"] - result["party_list_votes"]
    return result[columns]


def prepare_party_index_overall(candidate_votes, party_votes):
    area = prepare_party_area_index(candidate_votes, party_votes, "District")
    if area.empty:
        return area
    overall = (
        area.groupby("party_name", as_index=False)
        .agg(candidate_votes=("candidate_votes", "sum"), party_list_votes=("party_list_votes", "sum"))
    )
    candidate_total = overall["candidate_votes"].sum()
    party_total = overall["party_list_votes"].sum()
    overall["candidate_share_pct"] = np.where(
        candidate_total > 0,
        overall["candidate_votes"] / candidate_total * 100,
        0.0,
    )
    overall["party_list_share_pct"] = np.where(
        party_total > 0,
        overall["party_list_votes"] / party_total * 100,
        0.0,
    )
    overall["personal_vote_index_pp"] = overall["candidate_share_pct"] - overall["party_list_share_pct"]
    overall["abs_personal_vote_index_pp"] = overall["personal_vote_index_pp"].abs()
    overall["gap_votes"] = overall["candidate_votes"] - overall["party_list_votes"]
    return overall.sort_values("abs_personal_vote_index_pp", ascending=False)


def prepare_current_area_hotspots(stations, candidate_votes, party_votes, area_level, selected_party):
    station_summary = aggregate_station_map(stations, area_level)
    vote_summary = aggregate_vote_map(candidate_votes, area_level, "Constituency")
    result = station_summary.merge(vote_summary, on="area_key", how="outer")
    if selected_party and not party_votes.empty:
        party_keyed = add_area_key(party_votes, area_level)
        party_grouped = (
            party_keyed.groupby(["area_key", "entity_name"], as_index=False)["votes"]
            .sum()
        )
        totals = party_grouped.groupby("area_key")["votes"].sum().reset_index(name="party_total_votes")
        selected = (
            party_grouped[party_grouped["entity_name"] == selected_party]
            .rename(columns={"votes": "selected_party_votes"})[["area_key", "selected_party_votes"]]
        )
        result = result.merge(totals, on="area_key", how="left").merge(selected, on="area_key", how="left")
        result["selected_party_votes"] = result["selected_party_votes"].fillna(0)
        result["party_total_votes"] = result["party_total_votes"].fillna(0)
        result["selected_party_share_pct"] = np.where(
            result["party_total_votes"].gt(0),
            result["selected_party_votes"] / result["party_total_votes"] * 100,
            0.0,
        )
    else:
        result["selected_party_share_pct"] = 0.0
    return result


def prepare_party_swing(current_party_votes, election66_data, area_level, district, subdistrict):
    columns = [
        "area_key", "district", "subdistrict", "compare_party",
        "votes_2023", "votes_2026", "share_2023", "share_2026", "swing_pp",
    ]
    e66 = election66_data["party_area_long"]
    if current_party_votes.empty or e66.empty:
        return pd.DataFrame(columns=columns)

    current = add_area_key(current_party_votes, area_level)
    if current.empty:
        return pd.DataFrame(columns=columns)
    current["compare_party"] = party_compare_key(current["entity_name"])
    current_grouped = (
        current.groupby(["area_key", "district", "subdistrict", "compare_party"], as_index=False)["votes"]
        .sum()
        .rename(columns={"votes": "votes_2026"})
    )
    current_grouped["total_2026"] = current_grouped.groupby("area_key")["votes_2026"].transform("sum")
    current_grouped["share_2026"] = np.where(
        current_grouped["total_2026"].gt(0),
        current_grouped["votes_2026"] / current_grouped["total_2026"] * 100,
        0.0,
    )

    past = filter_election66_area(e66, district, subdistrict)
    if past.empty:
        return pd.DataFrame(columns=columns)
    past = add_area_key(past, area_level)
    past["compare_party"] = party_compare_key(past["party_name"])
    past_grouped = (
        past.groupby(["area_key", "district", "subdistrict", "compare_party"], as_index=False)["votes"]
        .sum()
        .rename(columns={"votes": "votes_2023"})
    )
    past_grouped["total_2023"] = past_grouped.groupby("area_key")["votes_2023"].transform("sum")
    past_grouped["share_2023"] = np.where(
        past_grouped["total_2023"].gt(0),
        past_grouped["votes_2023"] / past_grouped["total_2023"] * 100,
        0.0,
    )

    result = (
        past_grouped[["area_key", "district", "subdistrict", "compare_party", "votes_2023", "share_2023"]]
        .merge(
            current_grouped[["area_key", "district", "subdistrict", "compare_party", "votes_2026", "share_2026"]],
            on=["area_key", "district", "subdistrict", "compare_party"],
            how="outer",
        )
        .fillna({"votes_2023": 0, "share_2023": 0, "votes_2026": 0, "share_2026": 0})
    )
    result["swing_pp"] = result["share_2026"] - result["share_2023"]
    return result[columns]


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
        ["V1 — OCR + Imputed", "V2 — Proportional Scale", "V3 — Ground Truth", "V4 — KNN Imputed"],
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
      Constituency&nbsp;&nbsp;{cov_c:.2f}%<br>
      Party-list&nbsp;&nbsp;&nbsp;&nbsp;{cov_p:.2f}%<br>
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
    av, apv = f_v, f_pv


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
tab_map, tab1, tab2, tab3, tab4, tab66, tab_adv, tab5, tab6, tab7, tab8 = st.tabs([
    "Map", "Turnout", "Candidates", "Parties", "Split Vote",
    "2023 Compare", "Advanced Insights", "Station Size", "Versions", "Quality", "Missing"
])


# ─────────────────────────── TAB 0 · MAP ──────────────────────────────────────
with tab_map:
    st.subheader("Election Map")

    if ver == "V3":
        st.info("V3 is a constituency-level reference total. Use V1, V2, or V4 for area-level maps.")
    else:
        ctrl_a, ctrl_b, ctrl_c = st.columns(3)
        with ctrl_a:
            area_level = st.selectbox(
                "Area level",
                ["Sub-district", "District"],
                key="map_area_level",
            )
        with ctrl_b:
            vote_type = st.selectbox(
                "Vote type",
                ["Party-list", "Constituency"],
                key="map_vote_type",
            )
        with ctrl_c:
            map_metric = st.selectbox(
                "Map metric",
                ["Winner", "Turnout %", "Victory margin %", "Total votes"],
                key="map_metric",
            )

        geojson_path = SUBDISTRICT_GEOJSON if area_level == "Sub-district" else DISTRICT_GEOJSON
        geojson = load_geojson(str(geojson_path))
        vote_source = apv if vote_type == "Party-list" else av
        map_df = prepare_map_data(
            f_st, vote_source, geojson, area_level, sel_district, sel_sub, vote_type
        )

        if map_df.empty:
            empty_state(
                "No map areas match the selected filters",
                "Try selecting All for District or Sub-district.",
            )
        else:
            fig = build_map_figure(map_df, geojson, area_level, map_metric)
            st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)

            st.caption(
                "Map areas use local GeoJSON boundaries. Grey areas have no vote rows in the current filter scope."
            )
            table_cols = [
                "area_name", "district", "subdistrict", "stations", "turnout_pct",
                "winner", "winner_votes", "runner_up", "runner_up_votes",
                "margin_votes", "margin_pct", "total_votes",
            ]
            if area_level == "District":
                table_cols.remove("subdistrict")
            map_table = (
                map_df[table_cols]
                .rename(columns={
                    "area_name": "Area",
                    "district": "District",
                    "subdistrict": "Sub-district",
                    "stations": "Stations",
                    "turnout_pct": "Turnout %",
                    "winner": "Winner",
                    "winner_votes": "Winner Votes",
                    "runner_up": "Runner-up",
                    "runner_up_votes": "Runner-up Votes",
                    "margin_votes": "Margin",
                    "margin_pct": "Margin %",
                    "total_votes": "Total Votes",
                })
            )
            for col in ["Winner Votes", "Runner-up Votes", "Margin", "Total Votes"]:
                map_table[col] = map_table[col].round(0).astype(int)
            for col in ["Turnout %", "Margin %"]:
                map_table[col] = map_table[col].round(2)

            st.dataframe(
                map_table,
                width="stretch",
                hide_index=True,
            )


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
            fig.update_traces(hovertemplate="%{y}: <b>%{x:.2f}%</b>")
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
                texttemplate="%{percent:.2%}",
                hovertemplate="%{label}: <b>%{percent:.2%}</b><extra></extra>",
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
            texttemplate="%{percent:.2%}",
            hovertemplate="%{label}: <b>%{percent:.2%}</b><br>Count: %{value:,.0f}<extra></extra>",
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
        cand_total = cand_agg["votes"].sum()
        cand_agg["vote_share_pct"] = np.where(
            cand_total > 0,
            cand_agg["votes"] / cand_total * 100,
            0.0,
        )
        col_a, col_b = st.columns([3, 1])
        with col_a:
            fig = styled_bar(cand_agg, "votes", "entity_name",
                             f"Constituency Votes ({ver})", color=ORANGE, height=380)
            fig.update_traces(
                customdata=cand_agg[["vote_share_pct"]],
                hovertemplate="%{y}: <b>%{x:,.0f}</b> votes<br>Share: <b>%{customdata[0]:.2f}%</b><extra></extra>",
            )
            st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)

        with col_b:
            if len(cand_agg) >= 2:
                top2   = cand_agg.sort_values("votes", ascending=False).iloc[:2].reset_index(drop=True)
                winner = top2.loc[0, "entity_name"]
                winner_share = float(top2.loc[0, "vote_share_pct"])
                margin = int(top2.loc[0, "votes"] - top2.loc[1, "votes"])
                total  = int(cand_total)
                st.markdown(f"""
                <div class='insight-card' style='text-align:center;'>
                  <div style='color:{LGRAY}; font-size:0.78rem; font-weight:700;'>Winner</div>
                  <div style='color:{WHITE}; font-size:1.04rem; font-weight:760;
                              margin:8px 0'>{winner}</div>
                  <div style='color:{LGRAY}; font-size:0.78rem'>{winner_share:.2f}% of total votes</div>
                  <hr>
                  <div style='color:{LGRAY}; font-size:0.78rem'>Victory Margin</div>
                  <div style='color:{ORANGE}; font-size:1.58rem;
                              font-weight:820'>{margin:,}</div>
                  <div style='color:{LGRAY}; font-size:0.78rem'>
                    {margin/total*100:.2f}% of total votes
                  </div>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)
            cand_table = cand_agg.rename(
                columns={
                    "entity_name": "Candidate",
                    "votes": "Votes",
                    "vote_share_pct": "Share",
                }
            )[["Candidate", "Votes", "Share"]]
            cand_table["Votes"] = [
                f"{votes:,.0f}"
                for votes, share in zip(cand_table["Votes"], cand_table["Share"])
            ]
            cand_table["Share"] = cand_table["Share"].apply(lambda v: f"{v:.2f}%")
            cand_rows = "\n".join(
                "<tr>"
                f"<td>{html.escape(str(row['Candidate']))}</td>"
                f"<td><strong>{html.escape(str(row['Votes']))}</strong>"
                f"<span>{html.escape(str(row['Share']))}</span></td>"
                "</tr>"
                for _, row in cand_table.iterrows()
            )
            st.markdown(f"""
            <div class='candidate-share-table' style='border:1px solid {BORDER}; border-radius:8px; overflow:hidden;'>
              <table style='width:100%; border-collapse:collapse; font-size:0.78rem;'>
                <thead>
                  <tr style='background:rgba(255,255,255,0.045); color:{LGRAY};'>
                    <th style='padding:8px; text-align:left; font-weight:700;'>Candidate</th>
                    <th style='padding:8px; text-align:right; font-weight:700;'>Votes / Share</th>
                  </tr>
                </thead>
                <tbody>
                  {cand_rows}
                </tbody>
              </table>
            </div>
            <style>
              .candidate-share-table table td {{
                border-top: 1px solid rgba(255,255,255,0.08);
                padding: 8px;
                color: {WHITE};
                vertical-align: top;
              }}
              .candidate-share-table table td:first-child {{
                font-weight: 700;
                line-height: 1.25;
              }}
              .candidate-share-table table td:last-child {{
                text-align: right;
                white-space: nowrap;
                font-weight: 700;
              }}
              .candidate-share-table table td:last-child span {{
                display: block;
                color: {LGRAY};
                font-size: 0.72rem;
                margin-top: 2px;
              }}
            </style>
            """, unsafe_allow_html=True)


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
            fig = styled_party_bar(
                party_top, "votes", "entity_name",
                f"Party-List Votes — Top {top_n} ({ver})",
                height=420,
            )
            st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)

        with col_b:
            fig2 = px.pie(party_top, values="votes", names="entity_name",
                          title=f"Share — Top {top_n}",
                          color="entity_name",
                          color_discrete_map=party_color_map(party_top["entity_name"]),
                          hole=0.35)
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
                texttemplate="%{percent:.2%}",
                hovertemplate="%{label}: <b>%{percent:.2%}</b><br>Votes: %{value:,.0f}<extra></extra>",
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
            legend=right_side_legend(),
            height=430,
            margin=dict(l=8, r=160, t=46, b=36),
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


# ─────────────────────────── TAB 5 · ELECTION 2023 COMPARISON ────────────────
with tab66:
    st.subheader("Election 2023 vs 2026")

    has_election66 = not election66["candidates"].empty or not election66["party_totals"].empty
    if not has_election66:
        empty_state(
            "Election66 data is not available",
            "Run .venv/bin/python scripts/prepare_election66_chaiyaphum2.py after adding data/external/Election66.",
        )
    else:
        compare_scope = "Current scope"
        if sel_district == "All" and sel_sub == "All":
            compare_scope = "All areas"
        elif sel_sub != "All":
            compare_scope = f"{sel_district} · {sel_sub}"
        elif sel_district != "All":
            compare_scope = sel_district

        st.markdown(f"<span class='status-pill'>Scope <strong>{compare_scope}</strong></span>", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

        # Candidate comparison by the same candidate name across elections.
        e66_candidates = election66_candidate_totals(election66, sel_district, sel_sub)
        current_candidates = current_candidate_totals(av, ver)
        candidate_compare = (
            e66_candidates.merge(current_candidates, on="candidate_name", how="inner")
            .fillna(0)
        )

        if candidate_compare.empty:
            empty_state(
                "No overlapping candidate names",
                "The selected scope has no candidate names that appear in both election years.",
            )
        else:
            candidate_compare["vote_change"] = candidate_compare["votes_2026"] - candidate_compare["votes_2023"]
            candidate_total_2023 = e66_candidates["votes_2023"].sum()
            candidate_total_2026 = current_candidates["votes_2026"].sum()
            candidate_compare["share_2023"] = np.nan
            candidate_compare["share_2026"] = np.nan
            if candidate_total_2023 > 0:
                candidate_compare["share_2023"] = candidate_compare["votes_2023"] / candidate_total_2023 * 100
            if candidate_total_2026 > 0:
                candidate_compare["share_2026"] = candidate_compare["votes_2026"] / candidate_total_2026 * 100
            candidate_compare["candidate_label"] = (
                candidate_compare["candidate_name"] + " · "
                + candidate_compare["party_name_2023"].astype(str) + " → "
                + candidate_compare["party_name_2026"].astype(str)
            )
            candidate_compare = candidate_compare.sort_values("votes_2026", ascending=False)
            metric_col, _ = st.columns([1, 3])
            with metric_col:
                candidate_metric = st.segmented_control(
                    "Candidate metric",
                    options=["Votes", "Vote share %"],
                    default="Votes",
                    key="compare66_candidate_metric",
                )
            candidate_metric = candidate_metric or "Votes"
            candidate_year_2026 = f"2026 {ver}"

            candidate_long = candidate_compare.melt(
                id_vars=["candidate_label"],
                value_vars=["votes_2023", "votes_2026"],
                var_name="Year",
                value_name="Votes",
            )
            candidate_long["Year"] = candidate_long["Year"].replace({
                "votes_2023": "2023",
                "votes_2026": candidate_year_2026,
            })

            candidate_share_long = (
                candidate_compare.melt(
                    id_vars=["candidate_label"],
                    value_vars=["share_2023", "share_2026"],
                    var_name="Year",
                    value_name="Vote share %",
                )
                .dropna(subset=["Vote share %"])
            )
            candidate_share_long["Year"] = candidate_share_long["Year"].replace({
                "share_2023": "2023",
                "share_2026": candidate_year_2026,
            })

            if candidate_metric == "Vote share %":
                candidate_plot = candidate_share_long
                candidate_x = "Vote share %"
                candidate_title = "Constituency Vote Share by Same Candidate"
                candidate_xaxis = dict(
                    gridcolor=GRID,
                    zeroline=False,
                    title="Vote share (%)",
                    ticksuffix="%",
                    range=percent_axis_range(candidate_plot["Vote share %"]),
                    automargin=True,
                )
                candidate_hover = "%{y}<br>%{fullData.name}: <b>%{x:.2f}%</b><extra></extra>"
            else:
                candidate_plot = candidate_long
                candidate_x = "Votes"
                candidate_title = "Constituency Votes by Same Candidate"
                candidate_xaxis = dict(gridcolor=GRID, zeroline=False, title="Votes", automargin=True)
                candidate_hover = "%{y}<br>%{fullData.name}: <b>%{x:,.0f}</b><extra></extra>"

            if not candidate_plot.empty:
                fig = px.bar(
                    candidate_plot,
                    x=candidate_x,
                    y="candidate_label",
                    color="Year",
                    orientation="h",
                    barmode="group",
                    category_orders={"Year": [candidate_year_2026, "2023"]},
                    color_discrete_map={"2023": LIGHT_OG, candidate_year_2026: ORANGE},
                    title=candidate_title,
                )
                fig.update_layout(
                    plot_bgcolor=CHART_BG, paper_bgcolor=CHART_BG,
                    font_color=WHITE, title_font_color=WHITE,
                    xaxis=candidate_xaxis,
                    yaxis=dict(gridcolor=GRID, categoryorder="total ascending", zeroline=False, title="", automargin=True),
                    legend=right_side_legend(traceorder="reversed"),
                    height=430,
                    margin=dict(l=8, r=150, t=54, b=42),
                    hoverlabel=dict(bgcolor=PANEL_BG, font_color=WHITE, bordercolor=BORDER),
                )
                fig.update_traces(hovertemplate=candidate_hover)
                st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)

            e66_candidate_parties = e66_candidates.copy()
            if not e66_candidate_parties.empty:
                e66_candidate_parties["compare_party"] = party_compare_key(e66_candidate_parties["party_name_2023"])
                e66_candidate_parties = (
                    e66_candidate_parties.groupby("compare_party", as_index=False)["votes_2023"].sum()
                )
            else:
                e66_candidate_parties = pd.DataFrame(columns=["compare_party", "votes_2023"])

            current_candidate_parties = current_candidates.copy()
            if not current_candidate_parties.empty:
                current_candidate_parties["compare_party"] = party_compare_key(current_candidate_parties["party_name_2026"])
                current_candidate_parties = (
                    current_candidate_parties.groupby("compare_party", as_index=False)["votes_2026"].sum()
                )
            else:
                current_candidate_parties = pd.DataFrame(columns=["compare_party", "votes_2026"])

            candidate_party_compare = (
                e66_candidate_parties
                .merge(current_candidate_parties, on="compare_party", how="outer")
                .fillna({"votes_2023": 0, "votes_2026": 0})
            )
            if not candidate_party_compare.empty:
                candidate_party_compare["Party"] = candidate_party_compare["compare_party"].apply(party_compare_label)
                candidate_party_compare["share_2023"] = 0.0
                candidate_party_compare["share_2026"] = 0.0
                if candidate_total_2023 > 0:
                    candidate_party_compare["share_2023"] = (
                        candidate_party_compare["votes_2023"] / candidate_total_2023 * 100
                    )
                if candidate_total_2026 > 0:
                    candidate_party_compare["share_2026"] = (
                        candidate_party_compare["votes_2026"] / candidate_total_2026 * 100
                    )
                candidate_party_compare["total"] = (
                    candidate_party_compare["votes_2023"] + candidate_party_compare["votes_2026"]
                )
                candidate_party_compare = candidate_party_compare.sort_values("total", ascending=False).head(10)

                if candidate_metric == "Vote share %":
                    candidate_party_long = candidate_party_compare.melt(
                        id_vars=["Party"],
                        value_vars=["share_2023", "share_2026"],
                        var_name="Year",
                        value_name="Vote share %",
                    )
                    candidate_party_long["Year"] = candidate_party_long["Year"].replace({
                        "share_2023": "2023",
                        "share_2026": candidate_year_2026,
                    })
                    party_x = "Vote share %"
                    party_title = "Constituency Candidate Party Share"
                    party_xaxis = dict(
                        gridcolor=GRID,
                        zeroline=False,
                        title="Vote share (%)",
                        ticksuffix="%",
                        range=percent_axis_range(candidate_party_long["Vote share %"]),
                        automargin=True,
                    )
                    party_hover = "%{y}<br>%{fullData.name}: <b>%{x:.2f}%</b><extra></extra>"
                else:
                    candidate_party_long = candidate_party_compare.melt(
                        id_vars=["Party"],
                        value_vars=["votes_2023", "votes_2026"],
                        var_name="Year",
                        value_name="Votes",
                    )
                    candidate_party_long["Year"] = candidate_party_long["Year"].replace({
                        "votes_2023": "2023",
                        "votes_2026": candidate_year_2026,
                    })
                    party_x = "Votes"
                    party_title = "Constituency Candidate Votes by Party"
                    party_xaxis = dict(gridcolor=GRID, zeroline=False, title="Votes", automargin=True)
                    party_hover = "%{y}<br>%{fullData.name}: <b>%{x:,.0f}</b><extra></extra>"

                fig_party = px.bar(
                    candidate_party_long,
                    x=party_x,
                    y="Party",
                    color="Year",
                    orientation="h",
                    barmode="group",
                    category_orders={"Year": [candidate_year_2026, "2023"]},
                    color_discrete_map={"2023": LIGHT_OG, candidate_year_2026: ORANGE},
                    title=party_title,
                )
                fig_party.update_layout(
                    plot_bgcolor=CHART_BG, paper_bgcolor=CHART_BG,
                    font_color=WHITE, title_font_color=WHITE,
                    xaxis=party_xaxis,
                    yaxis=dict(gridcolor=GRID, categoryorder="total ascending", zeroline=False, title="", automargin=True),
                    legend=right_side_legend(traceorder="reversed"),
                    height=430,
                    margin=dict(l=8, r=150, t=54, b=42),
                    hoverlabel=dict(bgcolor=PANEL_BG, font_color=WHITE, bordercolor=BORDER),
                )
                fig_party.update_traces(hovertemplate=party_hover)
                st.plotly_chart(fig_party, width="stretch", config=PLOTLY_CONFIG)

            candidate_table = candidate_compare[[
                "candidate_name", "party_name_2023", "votes_2023",
                "party_name_2026", "votes_2026", "vote_change",
            ]].rename(columns={
                "candidate_name": "Candidate",
                "party_name_2023": "Party 2023",
                "votes_2023": "Votes 2023",
                "party_name_2026": "Party 2026",
                "votes_2026": f"Votes 2026 {ver}",
                "vote_change": "Change",
            })
            for col in ["Votes 2023", f"Votes 2026 {ver}", "Change"]:
                candidate_table[col] = candidate_table[col].apply(lambda value: f"{value:,.0f}")
            st.dataframe(candidate_table, width="stretch", hide_index=True)

        control_a, control_b, _ = st.columns([1, 1, 2])
        with control_a:
            top_n_66 = st.slider("Show top parties", 5, 20, 10, key="compare66_top_n")
        with control_b:
            party_metric = st.segmented_control(
                "Party-list metric",
                options=["Votes", "Vote share %"],
                default="Votes",
                key="compare66_party_metric",
            )
        party_metric = party_metric or "Votes"

        party_year_2026 = f"2026 {ver}"
        e66_parties = election66_party_totals(election66, sel_district, sel_sub)
        current_parties = current_party_totals(apv, ver)
        party_compare = (
            e66_parties.merge(current_parties, on="compare_party", how="outer")
            .fillna(0)
        )
        party_total_2023 = e66_parties["votes_2023"].sum()
        party_total_2026 = current_parties["votes_2026"].sum()
        party_compare["Party"] = party_compare["compare_party"].apply(party_compare_label)
        party_compare["share_2023"] = 0.0
        party_compare["share_2026"] = 0.0
        if party_total_2023 > 0:
            party_compare["share_2023"] = party_compare["votes_2023"] / party_total_2023 * 100
        if party_total_2026 > 0:
            party_compare["share_2026"] = party_compare["votes_2026"] / party_total_2026 * 100
        party_compare["total"] = party_compare["votes_2023"] + party_compare["votes_2026"]
        party_compare = party_compare.sort_values("total", ascending=False).head(top_n_66)

        if party_compare.empty:
            empty_state("No party-list comparison rows", "The selected scope has no party-list rows to compare.")
        else:
            party_long = party_compare.melt(
                id_vars=["Party"],
                value_vars=["votes_2023", "votes_2026"],
                var_name="Year",
                value_name="Votes",
            )
            party_long["Year"] = party_long["Year"].replace({
                "votes_2023": "2023",
                "votes_2026": party_year_2026,
            })
            party_share_long = party_compare.melt(
                id_vars=["Party"],
                value_vars=["share_2023", "share_2026"],
                var_name="Year",
                value_name="Vote share %",
            )
            party_share_long["Year"] = party_share_long["Year"].replace({
                "share_2023": "2023",
                "share_2026": party_year_2026,
            })

            if party_metric == "Vote share %":
                party_plot = party_share_long
                party_x = "Vote share %"
                party_title = f"Party-List Vote Share — Top {top_n_66}"
                party_xaxis = dict(
                    gridcolor=GRID,
                    zeroline=False,
                    title="Vote share (%)",
                    ticksuffix="%",
                    range=percent_axis_range(party_plot["Vote share %"]),
                    automargin=True,
                )
                party_hover = "%{y}<br>%{fullData.name}: <b>%{x:.2f}%</b><extra></extra>"
            else:
                party_plot = party_long
                party_x = "Votes"
                party_title = f"Party-List Votes — Top {top_n_66}"
                party_xaxis = dict(gridcolor=GRID, zeroline=False, title="Votes", automargin=True)
                party_hover = "%{y}<br>%{fullData.name}: <b>%{x:,.0f}</b><extra></extra>"

            fig2 = px.bar(
                party_plot,
                x=party_x,
                y="Party",
                color="Year",
                orientation="h",
                barmode="group",
                category_orders={"Year": [party_year_2026, "2023"]},
                color_discrete_map={"2023": LIGHT_OG, party_year_2026: ORANGE},
                title=party_title,
            )
            fig2.update_layout(
                plot_bgcolor=CHART_BG, paper_bgcolor=CHART_BG,
                font_color=WHITE, title_font_color=WHITE,
                xaxis=party_xaxis,
                yaxis=dict(gridcolor=GRID, categoryorder="total ascending", zeroline=False, title="", automargin=True),
                legend=right_side_legend(traceorder="reversed"),
                height=max(520, int(top_n_66) * 34 + 190),
                margin=dict(l=8, r=150, t=54, b=42),
                hoverlabel=dict(bgcolor=PANEL_BG, font_color=WHITE, bordercolor=BORDER),
            )
            fig2.update_traces(hovertemplate=party_hover)
            st.plotly_chart(fig2, width="stretch", config=PLOTLY_CONFIG)

        e66_turnout = election66_turnout_by_area(election66, sel_district, sel_sub)
        current_turnout = (
            f_st.groupby(["district", "subdistrict"], as_index=False)
            .agg(eligible=("eligible_voters", "sum"), present=("voters_present", "sum"))
        )
        current_turnout["turnout_2026"] = np.where(
            current_turnout["eligible"].gt(0),
            current_turnout["present"] / current_turnout["eligible"] * 100,
            np.nan,
        )
        turnout_compare = (
            e66_turnout.merge(current_turnout, on=["district", "subdistrict"], how="inner")
            .dropna(subset=["turnout_2023", "turnout_2026"])
        )
        if turnout_compare.empty:
            empty_state("No turnout comparison rows", "The selected scope has no matching area turnout rows.")
        else:
            turnout_compare["Area"] = turnout_compare["subdistrict"]
            turnout_compare["change"] = turnout_compare["turnout_2026"] - turnout_compare["turnout_2023"]
            turnout_compare = (
                turnout_compare.assign(abs_change=lambda d: d["change"].abs())
                .sort_values("abs_change", ascending=False)
                .head(12)
                .sort_values("turnout_2026")
            )
            turnout_long = turnout_compare.melt(
                id_vars=["Area"],
                value_vars=["turnout_2023", "turnout_2026"],
                var_name="Year",
                value_name="Turnout %",
            )
            turnout_long["Year"] = turnout_long["Year"].replace({
                "turnout_2023": "2023",
                "turnout_2026": "2026",
            })
            fig3 = px.bar(
                turnout_long,
                x="Turnout %",
                y="Area",
                color="Year",
                orientation="h",
                barmode="group",
                category_orders={"Year": ["2026", "2023"]},
                color_discrete_map={"2023": LIGHT_OG, "2026": ORANGE},
                title="Turnout % by Sub-district",
            )
            fig3.update_layout(
                plot_bgcolor=CHART_BG, paper_bgcolor=CHART_BG,
                font_color=WHITE, title_font_color=WHITE,
                xaxis=dict(gridcolor=GRID, zeroline=False, title="Turnout %", automargin=True),
                yaxis=dict(gridcolor=GRID, categoryorder="total ascending", zeroline=False, title="", automargin=True),
                legend=right_side_legend(traceorder="reversed"),
                height=520,
                margin=dict(l=8, r=150, t=54, b=42),
                hoverlabel=dict(bgcolor=PANEL_BG, font_color=WHITE, bordercolor=BORDER),
            )
            fig3.update_traces(hovertemplate="%{y}<br>%{fullData.name}: <b>%{x:.2f}%</b><extra></extra>")
            st.plotly_chart(fig3, width="stretch", config=PLOTLY_CONFIG)


# ─────────────────────────── TAB 6 · ADVANCED INSIGHTS ───────────────────────
with tab_adv:
    st.subheader("Advanced Insights")
    st.caption(
        "Exploratory analysis built from station-level vote patterns. "
        "Outlier scores are review signals, not evidence of wrongdoing."
    )

    if ver == "V3":
        st.info("Advanced station-level insights require V1, V2, or V4. V3 is a constituency-level reference total.")
    else:
        insight_outlier, insight_personal, insight_swing = st.tabs([
            "Outlier Review", "Personal Vote Index", "2023 Swing"
        ])

        with insight_outlier:
            anomaly_df = build_station_anomaly(f_st, av, apv)
            if anomaly_df.empty:
                empty_state(
                    "No station rows for outlier review",
                    "The selected filters returned no station-level rows to score.",
                )
            else:
                high_count = int(anomaly_df["review_score"].ge(75).sum())
                review_count = int(anomaly_df["review_score"].ge(55).sum())
                max_score = float(anomaly_df["review_score"].max())
                avg_score = float(anomaly_df["review_score"].mean())
                metric_a, metric_b, metric_c, metric_d = st.columns(4)
                metric_a.metric("High-priority stations", f"{high_count:,}")
                metric_b.metric("Review queue", f"{review_count:,}")
                metric_c.metric("Max score", f"{max_score:.1f}")
                metric_d.metric("Average score", f"{avg_score:.1f}")

                ctrl_a, ctrl_b = st.columns([1, 3])
                with ctrl_a:
                    anomaly_area_level = st.selectbox(
                        "Outlier map level",
                        ["Sub-district", "District"],
                        key="advanced_anomaly_area_level",
                    )

                geojson_path = SUBDISTRICT_GEOJSON if anomaly_area_level == "Sub-district" else DISTRICT_GEOJSON
                anomaly_geojson = load_geojson(str(geojson_path))
                anomaly_area = aggregate_anomaly_area(anomaly_df, anomaly_area_level)
                fig, anomaly_map_df = build_area_metric_map(
                    anomaly_area,
                    anomaly_geojson,
                    anomaly_area_level,
                    "max_review_score",
                    f"Outlier Review Score by {anomaly_area_level}",
                    "Review score",
                    color_scale=[PANEL_BG, LIGHT_OG, ORANGE, "#F87171"],
                )
                st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)

                feature_cols = [
                    "turnout_pct", "spoiled_pct", "no_vote_pct",
                    "winner_share_pct", "margin_pct", "split_abs_gap_pp",
                ]
                feature_summary = (
                    anomaly_df[feature_cols]
                    .rename(columns={
                        "turnout_pct": "Turnout %",
                        "spoiled_pct": "Spoiled %",
                        "no_vote_pct": "No-vote %",
                        "winner_share_pct": "Winner share %",
                        "margin_pct": "Margin %",
                        "split_abs_gap_pp": "Split gap pp",
                    })
                    .agg(["median", "max"])
                    .T
                    .reset_index()
                    .rename(columns={"index": "Feature", "median": "Median", "max": "Max"})
                )
                feature_summary["Median"] = feature_summary["Median"].round(2)
                feature_summary["Max"] = feature_summary["Max"].round(2)

                top_review = anomaly_df.head(20).copy()
                display_cols = [
                    "station_code", "district", "subdistrict", "review_score",
                    "review_level", "review_reason", "turnout_pct", "winner_candidate",
                    "winner_party", "winner_share_pct", "margin_pct",
                    "split_party", "split_abs_gap_pp", "validation_flags",
                ]
                top_review = top_review[display_cols].rename(columns={
                    "station_code": "Station",
                    "district": "District",
                    "subdistrict": "Sub-district",
                    "review_score": "Score",
                    "review_level": "Level",
                    "review_reason": "Reason",
                    "turnout_pct": "Turnout %",
                    "winner_candidate": "Winner",
                    "winner_party": "Winner Party",
                    "winner_share_pct": "Winner Share %",
                    "margin_pct": "Margin %",
                    "split_party": "Largest Split Party",
                    "split_abs_gap_pp": "Split Gap pp",
                    "validation_flags": "Validation Flags",
                })
                for col in ["Score", "Turnout %", "Winner Share %", "Margin %", "Split Gap pp"]:
                    top_review[col] = pd.to_numeric(top_review[col], errors="coerce").round(2)

                st.markdown("#### Top review stations")
                st.dataframe(top_review, width="stretch", hide_index=True)

                st.markdown("#### Feature range")
                st.dataframe(feature_summary, width="stretch", hide_index=True)

        with insight_personal:
            personal_overall = prepare_party_index_overall(av, apv)
            if personal_overall.empty:
                empty_state(
                    "No personal vote rows",
                    "This analysis needs both constituency and party-list votes in the selected scope.",
                )
            else:
                st.caption(
                    "Personal Vote Index = constituency candidate vote share - party-list vote share. "
                    "Positive values mean the constituency candidate outperformed the party-list vote."
                )
                top_personal = personal_overall.head(12).copy()
                fig = px.bar(
                    top_personal.sort_values("personal_vote_index_pp"),
                    x="personal_vote_index_pp",
                    y="party_name",
                    orientation="h",
                    color="personal_vote_index_pp",
                    color_continuous_scale=["#67A6FF", PANEL_BG, ORANGE],
                    color_continuous_midpoint=0,
                    title="Personal Vote Index by Party",
                    hover_data={
                        "candidate_share_pct": ":.2f",
                        "party_list_share_pct": ":.2f",
                        "gap_votes": ":,.0f",
                        "personal_vote_index_pp": ":.2f",
                    },
                )
                fig.update_layout(
                    plot_bgcolor=CHART_BG, paper_bgcolor=CHART_BG,
                    font_color=WHITE, title_font_color=WHITE,
                    coloraxis_showscale=False,
                    xaxis=dict(gridcolor=GRID, zeroline=True, zerolinecolor=LGRAY, title="Index (percentage points)", automargin=True),
                    yaxis=dict(gridcolor=GRID, title="", zeroline=False, automargin=True),
                    height=460,
                    margin=dict(l=10, r=30, t=54, b=54),
                    hoverlabel=dict(bgcolor=PANEL_BG, font_color=WHITE, bordercolor=BORDER),
                )
                fig.update_traces(
                    hovertemplate=(
                        "%{y}<br>Index: <b>%{x:.2f} pp</b>"
                        "<br>Candidate share: %{customdata[0]:.2f}%"
                        "<br>Party-list share: %{customdata[1]:.2f}%"
                        "<br>Vote gap: %{customdata[2]:,.0f}<extra></extra>"
                    )
                )
                st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)

                map_ctrl_a, map_ctrl_b = st.columns([1, 2])
                with map_ctrl_a:
                    personal_area_level = st.selectbox(
                        "Personal vote map level",
                        ["Sub-district", "District"],
                        key="advanced_personal_area_level",
                    )
                party_options = personal_overall["party_name"].tolist()
                with map_ctrl_b:
                    personal_party = st.selectbox(
                        "Party to map",
                        party_options,
                        key="advanced_personal_party",
                    )

                personal_area = prepare_party_area_index(av, apv, personal_area_level)
                if personal_area.empty:
                    empty_state(
                        "No area-level personal vote rows",
                        "The selected scope does not contain matching area rows.",
                    )
                else:
                    station_area = aggregate_station_map(f_st, personal_area_level)[["area_key", "stations"]]
                    personal_area = (
                        personal_area[personal_area["party_name"] == personal_party]
                        .merge(station_area, on="area_key", how="left")
                    )
                    personal_geojson = load_geojson(str(
                        SUBDISTRICT_GEOJSON if personal_area_level == "Sub-district" else DISTRICT_GEOJSON
                    ))
                    fig_map, personal_map_df = build_area_metric_map(
                        personal_area,
                        personal_geojson,
                        personal_area_level,
                        "personal_vote_index_pp",
                        f"{personal_party} Personal Vote Index by {personal_area_level}",
                        "Index pp",
                        color_scale=["#67A6FF", PANEL_BG, ORANGE],
                        midpoint=0,
                    )
                    st.plotly_chart(fig_map, width="stretch", config=PLOTLY_CONFIG)

                    personal_table = (
                        personal_area.sort_values("abs_personal_vote_index_pp", ascending=False)
                        .head(20)
                        .rename(columns={
                            "district": "District",
                            "subdistrict": "Sub-district",
                            "candidate_votes": "Candidate Votes",
                            "party_list_votes": "Party-list Votes",
                            "candidate_share_pct": "Candidate Share %",
                            "party_list_share_pct": "Party-list Share %",
                            "personal_vote_index_pp": "Index pp",
                            "gap_votes": "Vote Gap",
                        })
                    )
                    cols = [
                        "District", "Sub-district", "Candidate Votes", "Party-list Votes",
                        "Candidate Share %", "Party-list Share %", "Index pp", "Vote Gap",
                    ]
                    if personal_area_level == "District":
                        cols.remove("Sub-district")
                    personal_table = personal_table[cols]
                    for col in ["Candidate Votes", "Party-list Votes", "Vote Gap"]:
                        personal_table[col] = pd.to_numeric(personal_table[col], errors="coerce").round(0).astype(int)
                    for col in ["Candidate Share %", "Party-list Share %", "Index pp"]:
                        personal_table[col] = pd.to_numeric(personal_table[col], errors="coerce").round(2)
                    st.dataframe(personal_table, width="stretch", hide_index=True)

        with insight_swing:
            if election66["party_area_long"].empty:
                empty_state(
                    "No 2023 area rows",
                    "Run the Election66 preparation script to enable area-level swing analysis.",
                )
            else:
                e66_scope_parties = election66_party_totals(election66, sel_district, sel_sub)
                current_scope_parties = current_party_totals(apv, ver)
                swing_summary = (
                    e66_scope_parties.merge(current_scope_parties, on="compare_party", how="outer")
                    .fillna({"votes_2023": 0, "votes_2026": 0})
                )
                total_2023 = swing_summary["votes_2023"].sum()
                total_2026 = swing_summary["votes_2026"].sum()
                swing_summary["share_2023"] = np.where(
                    total_2023 > 0,
                    swing_summary["votes_2023"] / total_2023 * 100,
                    0.0,
                )
                swing_summary["share_2026"] = np.where(
                    total_2026 > 0,
                    swing_summary["votes_2026"] / total_2026 * 100,
                    0.0,
                )
                swing_summary["swing_pp"] = swing_summary["share_2026"] - swing_summary["share_2023"]
                swing_summary["Party"] = swing_summary["compare_party"].apply(party_compare_label)
                swing_summary = swing_summary.sort_values("swing_pp")

                if swing_summary.empty:
                    empty_state(
                        "No party swing rows",
                        "The selected scope has no party-list rows to compare.",
                    )
                else:
                    fig = px.bar(
                        swing_summary,
                        x="swing_pp",
                        y="Party",
                        orientation="h",
                        color="swing_pp",
                        color_continuous_scale=["#67A6FF", PANEL_BG, ORANGE],
                        color_continuous_midpoint=0,
                        title="Party-list Swing from 2023 to 2026",
                        hover_data={
                            "share_2023": ":.2f",
                            "share_2026": ":.2f",
                            "votes_2023": ":,.0f",
                            "votes_2026": ":,.0f",
                            "swing_pp": ":.2f",
                        },
                    )
                    fig.update_layout(
                        plot_bgcolor=CHART_BG, paper_bgcolor=CHART_BG,
                        font_color=WHITE, title_font_color=WHITE,
                        coloraxis_showscale=False,
                        xaxis=dict(gridcolor=GRID, zeroline=True, zerolinecolor=LGRAY, title="Swing (percentage points)", automargin=True),
                        yaxis=dict(gridcolor=GRID, title="", zeroline=False, automargin=True),
                        height=max(430, len(swing_summary) * 30 + 120),
                        margin=dict(l=10, r=30, t=54, b=54),
                        hoverlabel=dict(bgcolor=PANEL_BG, font_color=WHITE, bordercolor=BORDER),
                    )
                    fig.update_traces(
                        hovertemplate=(
                            "%{y}<br>Swing: <b>%{x:.2f} pp</b>"
                            "<br>2023 share: %{customdata[0]:.2f}%"
                            "<br>2026 share: %{customdata[1]:.2f}%"
                            "<br>2023 votes: %{customdata[2]:,.0f}"
                            "<br>2026 votes: %{customdata[3]:,.0f}<extra></extra>"
                        )
                    )
                    st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)

                    ctrl_a, ctrl_b = st.columns([1, 2])
                    with ctrl_a:
                        swing_area_level = st.selectbox(
                            "Swing map level",
                            ["Sub-district", "District"],
                            key="advanced_swing_area_level",
                        )
                    party_order = (
                        swing_summary.assign(abs_swing=lambda d: d["swing_pp"].abs())
                        .sort_values("abs_swing", ascending=False)
                    )
                    party_label_to_key = dict(zip(party_order["Party"], party_order["compare_party"]))
                    with ctrl_b:
                        swing_party_label = st.selectbox(
                            "Party to map",
                            list(party_label_to_key.keys()),
                            key="advanced_swing_party",
                        )
                    swing_party_key = party_label_to_key[swing_party_label]

                    swing_area = prepare_party_swing(
                        apv, election66, swing_area_level, sel_district, sel_sub
                    )
                    if swing_area.empty:
                        empty_state(
                            "No area-level swing rows",
                            "The selected scope has no matching 2023 and 2026 party-list area rows.",
                        )
                    else:
                        station_area = aggregate_station_map(f_st, swing_area_level)[["area_key", "stations"]]
                        swing_area = (
                            swing_area[swing_area["compare_party"] == swing_party_key]
                            .merge(station_area, on="area_key", how="left")
                        )
                        swing_geojson = load_geojson(str(
                            SUBDISTRICT_GEOJSON if swing_area_level == "Sub-district" else DISTRICT_GEOJSON
                        ))
                        fig_map, swing_map_df = build_area_metric_map(
                            swing_area,
                            swing_geojson,
                            swing_area_level,
                            "swing_pp",
                            f"{swing_party_label} Swing by {swing_area_level}",
                            "Swing pp",
                            color_scale=["#67A6FF", PANEL_BG, ORANGE],
                            midpoint=0,
                        )
                        st.plotly_chart(fig_map, width="stretch", config=PLOTLY_CONFIG)

                        swing_table = (
                            swing_area.sort_values("swing_pp", ascending=False)
                            .rename(columns={
                                "district": "District",
                                "subdistrict": "Sub-district",
                                "votes_2023": "Votes 2023",
                                "votes_2026": f"Votes 2026 {ver}",
                                "share_2023": "Share 2023 %",
                                "share_2026": f"Share 2026 {ver} %",
                                "swing_pp": "Swing pp",
                            })
                        )
                        cols = [
                            "District", "Sub-district", "Votes 2023", f"Votes 2026 {ver}",
                            "Share 2023 %", f"Share 2026 {ver} %", "Swing pp",
                        ]
                        if swing_area_level == "District":
                            cols.remove("Sub-district")
                        swing_table = swing_table[cols]
                        for col in ["Votes 2023", f"Votes 2026 {ver}"]:
                            swing_table[col] = pd.to_numeric(swing_table[col], errors="coerce").round(0).astype(int)
                        for col in ["Share 2023 %", f"Share 2026 {ver} %", "Swing pp"]:
                            swing_table[col] = pd.to_numeric(swing_table[col], errors="coerce").round(2)
                        st.dataframe(swing_table, width="stretch", hide_index=True)


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
                    color_discrete_map=party_color_map(pivot["entity_name"]),
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
                    legend=right_side_legend("Party"),
                    height=460,
                    margin=dict(l=28, r=150, t=76, b=58),
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
        legend=right_side_legend(),
        height=420, margin=dict(l=8, r=150, t=46, b=36),
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
        legend=right_side_legend(),
        height=460, margin=dict(l=8, r=150, t=46, b=36),
        hoverlabel=dict(bgcolor=PANEL_BG, font_color=WHITE, bordercolor=BORDER),
    )
    st.plotly_chart(fig2, width="stretch", config=PLOTLY_CONFIG)

    # ── Accuracy table ──
    st.subheader("OCR Accuracy vs Ground Truth")
    acc = comp_c.copy()
    acc["V1 acc %"] = (acc["V1"] / acc["V3"] * 100).round(2)
    acc["V2 acc %"] = (acc["V2"] / acc["V3"] * 100).round(2)
    acc["V4 acc %"] = (acc["V4"] / acc["V3"] * 100).round(2)
    st.dataframe(
        acc[["entity_name", "V1", "V2", "V3", "V4", "V1 acc %", "V2 acc %", "V4 acc %"]]
        .rename(columns={"entity_name": "Candidate"}),
        width="stretch", hide_index=True,
    )

    col_a, col_b = st.columns(2)
    col_a.metric("Constituency OCR Coverage",
                 f"{f_v['votes'].sum() / ref_cand_total * 100:.2f}%")
    col_b.metric("Party-List OCR Coverage",
                 f"{f_pv['votes'].sum() / ref_party_total * 100:.2f}%")


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
