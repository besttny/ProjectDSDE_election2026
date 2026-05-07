import streamlit as st
import pandas as pd
import numpy as np

# --- 1. Page Configuration ---
st.set_page_config(page_title="Thailand Election 2026 Dashboard", page_icon="🇹🇭", layout="wide")

# --- 2. Data Loading ---
@st.cache_data
def load_data():
    path = '../data/clean_data/' 
    station_df = pd.read_csv(f'{path}5_18_station.csv', encoding='utf-8-sig')
    votes_df = pd.read_csv(f'{path}5_18_votes.csv', encoding='utf-8-sig')
    party_votes_df = pd.read_csv(f'{path}5_18_party_vote.csv', encoding='utf-8-sig')
    
    return station_df, votes_df, party_votes_df

try:
    station_df, votes_df, party_votes_df = load_data()
except FileNotFoundError:
    st.error("⚠️ Could not find the clean data files. Please check your paths!")
    st.stop()

# --- 3. Sidebar Filters ---
st.sidebar.header("🔍 Filter Options")
subdistricts = ["All"] + list(station_df['subdistrict'].unique())
selected_sub = st.sidebar.selectbox("Select Sub-district", subdistricts)

if selected_sub != "All":
    filtered_stations = station_df[station_df['subdistrict'] == selected_sub]
    valid_station_codes = filtered_stations['station_code'].unique()
    filtered_votes = votes_df[votes_df['station_code'].isin(valid_station_codes)]
    filtered_party_votes = party_votes_df[party_votes_df['station_code'].isin(valid_station_codes)]
else:
    filtered_stations = station_df
    filtered_votes = votes_df
    filtered_party_votes = party_votes_df

# --- 4. Dashboard Header & KPIs ---
st.title("🇹🇭 Thailand Election 2026: Constituency Insights")

total_eligible = filtered_stations['eligible_voters'].sum()
total_present = filtered_stations['voters_present'].sum()
turnout_rate = (total_present / total_eligible) * 100 if total_eligible > 0 else 0
total_used = filtered_stations['ballots_used'].sum()
spoiled_rate = (filtered_stations['ballots_spoiled'].sum() / total_used) * 100 if total_used > 0 else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Eligible Voters", f"{total_eligible:,.0f}")
col2.metric("Voters Present", f"{total_present:,.0f}")
col3.metric("Turnout Rate", f"{turnout_rate:.2f}%")
col4.metric("Spoiled Ballot Rate", f"{spoiled_rate:.2f}%")

st.divider()

# --- 5. Dashboard Tabs ---
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Turnout", "🧑‍💼 Candidates", "🏛️ Parties", 
    "💡 Split-Voting", "🏘️ Demographics"
])

with tab1:
    st.subheader("Voter Turnout by Sub-district")
    turnout_by_sub = filtered_stations.groupby('subdistrict').agg(
        Total_Present=('voters_present', 'sum'),
        Total_Eligible=('eligible_voters', 'sum')
    ).reset_index()
    turnout_by_sub['Turnout %'] = (turnout_by_sub['Total_Present'] / turnout_by_sub['Total_Eligible']) * 100
    st.bar_chart(data=turnout_by_sub.set_index('subdistrict')['Turnout %'])

with tab2:
    st.subheader("Top Candidates in Constituency")
    candidate_totals = filtered_votes.groupby('entity_name')['votes'].sum().sort_values(ascending=False).head(10)
    st.bar_chart(candidate_totals)

with tab3:
    st.subheader("Top Parties (Party-List Vote)")
    party_totals = filtered_party_votes.groupby('entity_name')['votes'].sum().sort_values(ascending=False).head(10)
    st.bar_chart(party_totals)

with tab4:
    st.subheader("Split-Ticket Voting Analysis")
    cand_by_party = filtered_votes.groupby('party_name')['votes'].sum().reset_index()
    cand_by_party.rename(columns={'party_name': 'Party', 'votes': 'Candidate_Votes'}, inplace=True)
    party_list = filtered_party_votes.groupby('entity_name')['votes'].sum().reset_index()
    party_list.rename(columns={'entity_name': 'Party', 'votes': 'Party_List_Votes'}, inplace=True)
    
    comparison_df = pd.merge(cand_by_party, party_list, on='Party', how='outer').fillna(0)
    comparison_df['Vote_Gap'] = comparison_df['Candidate_Votes'] - comparison_df['Party_List_Votes']
    comparison_df['Total_Combined'] = comparison_df['Candidate_Votes'] + comparison_df['Party_List_Votes']
    
    top_10 = comparison_df.sort_values('Total_Combined', ascending=False).head(10).set_index('Party')
    st.bar_chart(top_10[['Candidate_Votes', 'Party_List_Votes']])
    st.dataframe(top_10[['Candidate_Votes', 'Party_List_Votes', 'Vote_Gap']], use_container_width=True)

with tab5:
    st.subheader("Voting Preference by Polling Station Size")
    st.markdown("Are large/urban stations voting differently than small/rural ones?")
    # Create bins dynamically based on filtered data
    filtered_stations['station_size'] = pd.qcut(filtered_stations['eligible_voters'].rank(method='first'), q=3, labels=['Small', 'Medium', 'Large'])
    party_with_size = pd.merge(filtered_party_votes, filtered_stations[['station_code', 'station_size']], on='station_code', how='inner')
    
    # Get top 5 parties to keep chart readable
    party_totals = filtered_party_votes.groupby('entity_name')['votes'].sum().sort_values(ascending=False).head(10)
    top_5_parties = party_totals.head(5).index.tolist()
    size_preference = party_with_size[party_with_size['entity_name'].isin(top_5_parties)]
    
    pivot_size = size_preference.pivot_table(index='station_size', columns='entity_name', values='votes', aggfunc='sum')
    st.bar_chart(pivot_size)

# --- 6. Data Quality Footer ---
st.divider()
with st.expander("🛠️ Data Quality & Validation Log"):
    st.markdown("Log of stations requiring OCR validation or imputation [Component 1].")
    flagged_data = station_df[station_df['validation_flags'].notna()]
    st.dataframe(flagged_data[['station_code', 'district', 'subdistrict', 'validation_flags']])