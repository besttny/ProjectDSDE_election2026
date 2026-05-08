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
ORANGE     = "#FF6B35"
LIGHT_OG   = "#FFB347"
DARK_OG    = "#E85D1F"
CARD_BG    = "#1A1A1A"
WHITE      = "#FFFFFF"
LGRAY      = "#AAAAAA"
PALETTE    = [ORANGE, LIGHT_OG, DARK_OG, "#FFA500", "#FF8C00",
              "#FF7043", "#FF5722", "#F4511E", "#E64A19", "#BF360C"]

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
  /* Metric cards */
  [data-testid="metric-container"] {{
      background: {CARD_BG};
      border: 1px solid {ORANGE};
      border-radius: 10px;
      padding: 14px 18px;
  }}
  [data-testid="metric-container"] label {{
      color: {LGRAY} !important;
      font-size: 0.82rem !important;
  }}
  [data-testid="stMetricValue"] {{
      color: {ORANGE} !important;
      font-size: 1.55rem !important;
      font-weight: 700 !important;
  }}
  /* Tabs */
  .stTabs [data-baseweb="tab-list"] {{ gap: 6px; }}
  .stTabs [data-baseweb="tab"] {{
      background: {CARD_BG};
      border-radius: 8px 8px 0 0;
      padding: 8px 18px;
      color: {LGRAY};
      font-weight: 500;
  }}
  .stTabs [aria-selected="true"] {{
      background: {ORANGE} !important;
      color: {WHITE} !important;
  }}
  /* Headings */
  h1, h2, h3, h4 {{ color: {ORANGE} !important; }}
  /* Sidebar select labels */
  .stSelectbox > label, .stSlider > label {{ color: {LGRAY} !important; }}
  /* Divider */
  hr {{ border-color: {ORANGE}; opacity: 0.3; }}
</style>
""", unsafe_allow_html=True)

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
    return station, votes, p_station, p_votes, cand_ref, party_ref


try:
    station_df, votes_df, p_station_df, p_votes_df, cand_ref, party_ref = load_data()
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
    fig.update_layout(
        plot_bgcolor=CARD_BG, paper_bgcolor=CARD_BG,
        font_color=WHITE, title_font_color=ORANGE,
        height=height,
        xaxis=dict(gridcolor="#2A2A2A", showgrid=True),
        yaxis=dict(gridcolor="#2A2A2A", categoryorder="total ascending"),
        margin=dict(l=8, r=8, t=40, b=8),
    )
    fig.update_traces(hovertemplate="%{y}: <b>%{x:,.0f}</b> votes")
    return fig


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
        f"<h2 style='color:{ORANGE}; margin-bottom:0'>🗳️ Election 2026</h2>"
        f"<p style='color:{LGRAY}; margin-top:2px; font-size:0.9rem'>ชัยภูมิ เขต 2</p>",
        unsafe_allow_html=True,
    )
    st.divider()

    version = st.selectbox(
        "📊 Data Version",
        ["V1 — OCR + Imputed", "V2 — Proportional Scale", "V3 — Ground Truth", "🔀 Compare All"],
    )
    ver = version.split("—")[0].strip()

    st.divider()

    districts = ["All"] + sorted(station_df["district"].dropna().unique())
    sel_district = st.selectbox("🏙️ District", districts)

    sub_pool = (
        station_df[station_df["district"] == sel_district]["subdistrict"]
        if sel_district != "All"
        else station_df["subdistrict"]
    )
    subs = ["All"] + sorted(sub_pool.dropna().unique())
    sel_sub = st.selectbox("📍 Sub-district", subs)

    st.divider()
    cov_c = votes_df["votes"].sum() / ref_cand_total * 100
    cov_p = p_votes_df["votes"].sum() / ref_party_total * 100
    st.markdown(f"""
    <div style='color:{LGRAY}; font-size:0.78rem; line-height:1.8'>
      <span style='color:{ORANGE}; font-weight:700'>OCR Coverage</span><br>
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

# Active vote dfs for single-version tabs
if ver == "V1":
    av, apv = f_v, f_pv
elif ver == "V2":
    av, apv = f_v2, f_v2p
elif ver == "V3":
    av, apv = v3_cand, v3_party   # province-level only
else:
    av, apv = f_v, f_pv           # Compare All → use V1 as base


# ── Header ─────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style='text-align:center; padding:10px 0 4px'>
  <h1 style='font-size:2.1rem; margin-bottom:2px'>
    🇹🇭 Thailand General Election 2026
  </h1>
  <p style='color:{LGRAY}; font-size:1rem; margin-top:0'>
    Chaiyaphum Province · Constituency 2 ·
    <span style='background:{ORANGE}; color:{WHITE}; padding:3px 12px;
                 border-radius:20px; font-size:0.85rem; font-weight:700'>{ver}</span>
  </p>
</div>
""", unsafe_allow_html=True)

st.divider()

# ── KPI row ────────────────────────────────────────────────────────────────────
total_elig   = f_st["eligible_voters"].sum()
total_pres   = f_st["voters_present"].sum()
turnout_pct  = total_pres / total_elig * 100 if total_elig else 0
total_used   = f_st["ballots_used"].sum()
spoiled_pct  = f_st["ballots_spoiled"].sum() / total_used * 100 if total_used else 0

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("🏛️ Stations",        f"{len(f_st):,}")
c2.metric("👥 Eligible Voters",  f"{total_elig:,.0f}")
c3.metric("✅ Voters Present",   f"{total_pres:,.0f}")
c4.metric("📊 Turnout",          f"{turnout_pct:.2f}%")
c5.metric("❌ Spoiled Ballots",  f"{spoiled_pct:.2f}%")

st.divider()

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📊 Turnout", "🧑‍💼 Candidates", "🏛️ Parties",
    "💡 Split-Voting", "🏘️ Demographics", "📈 Compare Versions", "🛠️ Data Quality",
])


# ─────────────────────────── TAB 1 · TURNOUT ──────────────────────────────────
with tab1:
    if ver == "V3":
        st.info("Station-level turnout is only available for V1 / V2.")
    else:
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
                         color="pct", color_continuous_scale=["#1A1A1A", ORANGE])
            fig.update_layout(
                plot_bgcolor=CARD_BG, paper_bgcolor=CARD_BG,
                font_color=WHITE, title_font_color=ORANGE,
                coloraxis_showscale=False,
                xaxis=dict(gridcolor="#2A2A2A", title="Turnout %"),
                yaxis=dict(gridcolor="#2A2A2A", title=""),
                margin=dict(l=8, r=8, t=40, b=8),
            )
            fig.update_traces(hovertemplate="%{y}: <b>%{x:.1f}%</b>")
            st.plotly_chart(fig, use_container_width=True)

        with col_b:
            dist_share = (
                f_st.groupby("district")["voters_present"].sum().reset_index()
            )
            fig2 = px.pie(dist_share, values="voters_present", names="district",
                          title="Voter Share by District",
                          color_discrete_sequence=PALETTE, hole=0.4)
            fig2.update_layout(
                plot_bgcolor=CARD_BG, paper_bgcolor=CARD_BG,
                font_color=WHITE, title_font_color=ORANGE,
                legend=dict(font=dict(color=WHITE)),
                margin=dict(l=8, r=8, t=40, b=8),
            )
            st.plotly_chart(fig2, use_container_width=True)

        # Ballot health
        st.subheader("Ballot Breakdown")
        ballot_df = pd.DataFrame({
            "Type":  ["Good Ballots", "Spoiled", "No Vote"],
            "Count": [f_st["ballots_good"].sum(),
                      f_st["ballots_spoiled"].sum(),
                      f_st["ballots_no_vote"].sum()],
        })
        fig3 = px.pie(ballot_df, values="Count", names="Type",
                      color_discrete_sequence=[ORANGE, "#555", LIGHT_OG], hole=0.4)
        fig3.update_layout(
            plot_bgcolor=CARD_BG, paper_bgcolor=CARD_BG,
            font_color=WHITE, title_font_color=ORANGE,
            legend=dict(font=dict(color=WHITE)),
        )
        st.plotly_chart(fig3, use_container_width=True)


# ─────────────────────────── TAB 2 · CANDIDATES ───────────────────────────────
with tab2:
    if ver == "V3":
        cand_agg = v3_cand[["entity_name", "votes"]].copy()
    else:
        cand_agg = (
            av.groupby("entity_name")["votes"].sum()
            .reset_index().sort_values("votes", ascending=False)
        )

    col_a, col_b = st.columns([3, 1])
    with col_a:
        fig = styled_bar(cand_agg, "votes", "entity_name",
                         f"Constituency Votes ({ver})", color=ORANGE, height=380)
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        if len(cand_agg) >= 2:
            top2   = cand_agg.sort_values("votes", ascending=False).iloc[:2].reset_index(drop=True)
            winner = top2.loc[0, "entity_name"]
            margin = int(top2.loc[0, "votes"] - top2.loc[1, "votes"])
            total  = int(cand_agg["votes"].sum())
            st.markdown(f"""
            <div style='background:{CARD_BG}; border:1px solid {ORANGE};
                        border-radius:12px; padding:20px; text-align:center;'>
              <div style='color:{LGRAY}; font-size:0.78rem; text-transform:uppercase;
                          letter-spacing:1px'>🏆 Winner</div>
              <div style='color:{ORANGE}; font-size:1rem; font-weight:700;
                          margin:8px 0'>{winner}</div>
              <hr style='border-color:{ORANGE}; opacity:0.3'>
              <div style='color:{LGRAY}; font-size:0.78rem'>Victory Margin</div>
              <div style='color:{WHITE}; font-size:1.5rem;
                          font-weight:800'>{margin:,}</div>
              <div style='color:{LGRAY}; font-size:0.78rem'>
                {margin/total*100:.1f}% of total votes
              </div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        st.dataframe(
            cand_agg.rename(columns={"entity_name": "Candidate", "votes": "Votes"})
                    .assign(Votes=lambda d: d["Votes"].apply(lambda v: f"{v:,.0f}")),
            use_container_width=True, hide_index=True,
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

    col_a, col_b = st.columns([3, 2])
    with col_a:
        fig = styled_bar(party_top, "votes", "entity_name",
                         f"Party-List Votes — Top {top_n} ({ver})",
                         color=LIGHT_OG, height=420)
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        fig2 = px.pie(party_top, values="votes", names="entity_name",
                      title=f"Share — Top {top_n}",
                      color_discrete_sequence=PALETTE, hole=0.35)
        fig2.update_layout(
            plot_bgcolor=CARD_BG, paper_bgcolor=CARD_BG,
            font_color=WHITE, title_font_color=ORANGE,
            legend=dict(font=dict(color=WHITE)),
            margin=dict(l=8, r=8, t=40, b=8),
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.dataframe(
        party_top.rename(columns={"entity_name": "Party", "votes": "Votes"})
                 .assign(Votes=lambda d: d["Votes"].apply(lambda v: f"{v:,.0f}")),
        use_container_width=True, hide_index=True,
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

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Constituency Vote", y=split["Party"], x=split["Candidate_Votes"],
        orientation="h", marker_color=ORANGE,
    ))
    fig.add_trace(go.Bar(
        name="Party-List Vote", y=split["Party"], x=split["Party_List_Votes"],
        orientation="h", marker_color=LIGHT_OG,
    ))
    fig.update_layout(
        barmode="group", title="Split-Ticket Voting — Top 10 Parties",
        plot_bgcolor=CARD_BG, paper_bgcolor=CARD_BG,
        font_color=WHITE, title_font_color=ORANGE,
        xaxis=dict(gridcolor="#2A2A2A"),
        yaxis=dict(gridcolor="#2A2A2A", categoryorder="total ascending"),
        legend=dict(font=dict(color=WHITE)),
        height=430,
        margin=dict(l=8, r=8, t=40, b=8),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.caption(
        "✅ Positive gap = candidate outperformed party   "
        "❌ Negative gap = party outperformed candidate"
    )
    st.dataframe(
        split[["Party", "Candidate_Votes", "Party_List_Votes", "Vote_Gap"]],
        use_container_width=True, hide_index=True,
    )


# ─────────────────────────── TAB 5 · DEMOGRAPHICS ─────────────────────────────
with tab5:
    if ver == "V3":
        st.info("Demographics analysis requires station-level data (V1 / V2 only).")
    else:
        st.subheader("Voting Preference by Station Size")
        st.caption("Stations grouped into Small / Medium / Large by eligible voter count")

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
        
        fig = px.bar(
            pivot, x="station_size", y="votes", color="entity_name",
            barmode="group", title="Top 5 Parties by Station Size",
            color_discrete_sequence=PALETTE,
            category_orders={
                "station_size": ["Small", "Medium", "Large"],
                "entity_name": top5  # <-- Fix: Forces bars to render highest to lowest
            },
        )
        
        fig.update_layout(
            plot_bgcolor=CARD_BG, paper_bgcolor=CARD_BG,
            font_color=WHITE, title_font_color=ORANGE,
            xaxis=dict(gridcolor="#2A2A2A"),
            yaxis=dict(gridcolor="#2A2A2A"),
            legend=dict(font=dict(color=WHITE), title_font_color=ORANGE),
            height=420,
        )
        st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────── TAB 6 · COMPARE VERSIONS ─────────────────────────
with tab6:
    st.subheader("V1 vs V2 vs V3 — Side by Side")

    # ── Candidate comparison ──
    v1c = f_v.groupby("entity_name")["votes"].sum().reset_index().rename(columns={"votes": "V1"})
    v2c = f_v2.groupby("entity_name")["votes"].sum().reset_index().rename(columns={"votes": "V2"})
    v3c = v3_cand[["entity_name", "votes"]].rename(columns={"votes": "V3"})
    comp_c = (
        v1c.merge(v2c, on="entity_name", how="outer")
           .merge(v3c, on="entity_name", how="outer")
           .sort_values("V3", ascending=False).reset_index(drop=True)
    )

    fig = go.Figure()
    for col, color, name in [
        ("V1", ORANGE,   "V1 — OCR + Imputed"),
        ("V2", LIGHT_OG, "V2 — Proportional"),
        ("V3", WHITE,    "V3 — Ground Truth"),
    ]:
        fig.add_trace(go.Bar(
            name=name, y=comp_c["entity_name"], x=comp_c[col],
            orientation="h", marker_color=color,
        ))
    fig.update_layout(
        barmode="group", title="Constituency Votes — All Versions",
        plot_bgcolor=CARD_BG, paper_bgcolor=CARD_BG,
        font_color=WHITE, title_font_color=ORANGE,
        xaxis=dict(gridcolor="#2A2A2A"),
        yaxis=dict(gridcolor="#2A2A2A", categoryorder="total ascending"),
        legend=dict(font=dict(color=WHITE)),
        height=420, margin=dict(l=8, r=8, t=40, b=8),
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Party comparison ──
    v1p = f_pv.groupby("entity_name")["votes"].sum().reset_index().rename(columns={"votes": "V1"})
    v2p = f_v2p.groupby("entity_name")["votes"].sum().reset_index().rename(columns={"votes": "V2"})
    v3p = v3_party[["entity_name", "votes"]].rename(columns={"votes": "V3"})
    comp_p = (
        v1p.merge(v2p, on="entity_name", how="outer")
           .merge(v3p, on="entity_name", how="outer")
           .sort_values("V3", ascending=False).head(10).reset_index(drop=True)
    )

    fig2 = go.Figure()
    for col, color, name in [
        ("V1", ORANGE,   "V1 — OCR + Imputed"),
        ("V2", LIGHT_OG, "V2 — Proportional"),
        ("V3", WHITE,    "V3 — Ground Truth"),
    ]:
        fig2.add_trace(go.Bar(
            name=name, y=comp_p["entity_name"], x=comp_p[col],
            orientation="h", marker_color=color,
        ))
    fig2.update_layout(
        barmode="group", title="Party-List Votes — Top 10, All Versions",
        plot_bgcolor=CARD_BG, paper_bgcolor=CARD_BG,
        font_color=WHITE, title_font_color=ORANGE,
        xaxis=dict(gridcolor="#2A2A2A"),
        yaxis=dict(gridcolor="#2A2A2A", categoryorder="total ascending"),
        legend=dict(font=dict(color=WHITE)),
        height=460, margin=dict(l=8, r=8, t=40, b=8),
    )
    st.plotly_chart(fig2, use_container_width=True)

    # ── Accuracy table ──
    st.subheader("OCR Accuracy vs Ground Truth")
    acc = comp_c.copy()
    acc["V1 acc %"] = (acc["V1"] / acc["V3"] * 100).round(1)
    acc["V2 acc %"] = (acc["V2"] / acc["V3"] * 100).round(1)
    st.dataframe(
        acc[["entity_name", "V1", "V2", "V3", "V1 acc %", "V2 acc %"]]
        .rename(columns={"entity_name": "Candidate"}),
        use_container_width=True, hide_index=True,
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

    flag_counts = (
        flagged["validation_flags"]
        .str.split(";").explode()
        .str.split(":").str[0]
        .str.strip("|")
        .value_counts().reset_index()
    )
    flag_counts.columns = ["Flag", "Count"]

    fig = styled_bar(flag_counts.head(12), "Count", "Flag",
                     "Top Validation Flags", color=DARK_OG, height=380)
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        flagged[["station_code", "district", "subdistrict", "validation_flags"]],
        use_container_width=True, hide_index=True,
    )
