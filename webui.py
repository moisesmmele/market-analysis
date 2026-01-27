from jobspy_processor import JobspyProcessor
from streamlit import session_state
from database import Database
import streamlit as st
import altair as alt

from session import Session

db = Database()

import pandas as pd

def make_chart(counts: dict, limit: int = 30):
    data = pd.DataFrame(list(counts.items()), columns=["keyword", "count"])
    data = data.sort_values("count", ascending=False).head(limit)
    
    return alt.Chart(data).mark_bar().encode(
        x=alt.X("keyword", sort="-y", axis=alt.Axis(labelAngle=-45, title=None)),
        y=alt.Y("count", title="Count"),
        tooltip=["keyword", "count"]
    ).interactive()

def render_dashboard(results, limit: int = 30):
    for item in results:
        topic = item.get("topic", "Unknown Topic")
        description = item.get("description", "")
        
        with st.container():
            st.subheader(topic)
            if description:
                st.caption(description)
            
            tab_overview, tab_levels = st.tabs(["Overview", "By Job Level"])
            
            with tab_overview:
                total_counts = item.get("total", {}).get("counts", {})
                if total_counts:
                    st.altair_chart(make_chart(total_counts, limit), use_container_width=True)
                    st.markdown(f"**Total Listings:** {item.get('total', {}).get('listings', 0)}")
                else:
                    st.info("No data available for this topic.")
            
            with tab_levels:
                levels_data = item.get("filtered_by_job_level", {})
                if levels_data:
                    # Use a unique key for each widget to avoid conflicts
                    selected_level = st.radio(
                        "Select Job Level", 
                        options=list(levels_data.keys()), 
                        horizontal=True,
                        key=f"radio_{topic}",
                        label_visibility="collapsed"
                    )
                    
                    if selected_level:
                        level_info = levels_data[selected_level]
                        counts = level_info.get("counts", {})
                        listings_count = level_info.get("listings", 0)
                        
                        if counts:
                            st.altair_chart(make_chart(counts, limit), use_container_width=True)
                            st.markdown(f"**Listings at this level:** {listings_count}")
                        else:
                            st.warning(f"No keywords found for {selected_level} level.")
                else:
                    st.info("No job level data available.")
            
            st.divider()

st.set_page_config(page_title="Job Market Analysis", page_icon="💀", layout="wide")
st.title("💀 Job Market Analysis")
st.space()

# --- Sidebar: Session Selection ---
with st.sidebar:
    st.header("📂 Sessions")
    index: dict[int, str] = db.get_index()
    
    if not index:
        st.warning("No sessions found. Run the scraper via CLI first.")
        st.stop()

    current_id = st.session_state.get('current_session')
    default_index = list(index.keys()).index(current_id) if current_id in index else 0

    selected_session: int = st.selectbox(
        "Select Session",
        options=index.keys(),
        format_func=lambda option_id: f"{option_id}. {index[option_id]}",
        index=default_index
    )

    if st.button("Load Session", type="primary"):
        session = db.get_session(selected_session)
        st.session_state['current_session'] = session
        st.rerun()
    
    st.divider()
    st.header("⚙️ Settings")
    top_n = st.number_input("Top Keywords to Display", min_value=5, max_value=100, value=30, step=5)

if 'current_session' in st.session_state:
    session = st.session_state['current_session']
    processor = JobspyProcessor(session)
    results = processor.process()

    with st.container():
        st.subheader(f"📄 Session: {session.title}")
        cols = st.columns(3)
        cols[0].metric("Session ID", session.id)
        cols[1].metric("Date", session.start_time.strftime("%Y-%m-%d %H:%M"))
        cols[2].metric("Total Listings", len(session.listings) if session.listings else 0)
        
        st.info(session.description)
        
        with st.expander("Show Metadata"):
            st.json(session.meta)
    
    st.divider()
    
    if results:
        render_dashboard(results, limit=top_n)
    else:
        st.info("No analysis results generated.")

else:
    st.info("👈 Please select a session from the sidebar to view results.")
    st.stop()