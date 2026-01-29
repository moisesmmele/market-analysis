from processor import JobspyProcessor
from topic_loader import TopicLoader
from database import Database
import streamlit as st
import altair as alt
import pandas as pd

#"singletons" using streamlit cache
@st.cache_resource
def get_database() -> Database:
    return Database()
db = get_database()

@st.cache_resource
def get_topic_loader() -> TopicLoader:
    return TopicLoader()
topic_loader = get_topic_loader()


def make_chart(counts: dict, total_listings: int, limit: int = 30):
    data = pd.DataFrame(list(counts.items()), columns=["keyword", "count"])
    
    if total_listings > 0:
        data["percentage"] = (data["count"] / total_listings * 100).round(1)
    else:
        data["percentage"] = 0.0
        
    data = data.sort_values("count", ascending=False).head(limit)
    data["label"] = data["percentage"].map('{:.1f}%'.format)
    
    # Create base chart with common elements
    base = alt.Chart(data).encode(
        x=alt.X("keyword", sort=None, axis=alt.Axis(labelAngle=-45, title=None)),
        tooltip=[
            alt.Tooltip("keyword", title="Keyword"),
            alt.Tooltip("count", title="Count"),
            alt.Tooltip("percentage", title="Percentage (%)", format=".1f")
        ]
    )

    # Calculate max percentage for Y-axis domain
    max_val = data["percentage"].max()
    
    # Global Percentage Bars
    bars = base.mark_bar().encode(
        y=alt.Y("percentage", title="Percentage (%)", scale=alt.Scale(domain=[0, max_val + 5]))
    )

    # Percentage Labels
    chart = bars
    max_bars_for_labels = 25
    if len(data) <= max_bars_for_labels:
        text = base.mark_text(dy=-10, fontSize=16, color='gray', fontWeight='bold').encode(
            y=alt.Y("percentage"),
            text=alt.Text("label")
        )
        chart = bars + text

    return chart.interactive()

def render_dashboard(results, limit: int = 30):
    for item in results:
        topic = item.get("topic", "Unknown Topic")
        description = item.get("description", "")
        
        with st.container():
            st.subheader(topic)
            if description:
                st.caption(description)
            
            # Prepare data and tabs
            levels_data = item.get("filtered_by_job_level", {})
            level_names = list(levels_data.keys())
            
            # Create tabs: first is Overview, then one for each level
            tab_names = ["Overview"] + level_names
            tabs = st.tabs(tab_names)
            
            # Tab 0: Overview
            with tabs[0]:
                total_data = item.get("total", {})
                total_counts = total_data.get("counts", {})
                total_listings = total_data.get("listings", 0)
                total_matched = total_data.get("matched", 0)
                
                if total_counts:
                    st.altair_chart(make_chart(total_counts, total_listings, limit), use_container_width=True)
                    
                    per_level = total_data.get("per_level", {})
                    
                    # Columns: Total Listings + (each level in per_level) + Matched
                    main_cols = st.columns([1, 2])
                    
                    # 1. General Column
                    with main_cols[0]:
                        st.markdown("**General Metrics**")
                        # Using 2 columns for Total and Matched
                        gen_cols = st.columns(2)
                        gen_cols[0].metric("Total", total_listings)
                        gen_cols[1].metric("Matched Keywords", total_matched)
                    
                    # 2. Distribution Column
                    with main_cols[1]:
                        st.markdown("**Distribution by Level**")
                        # Dynamic columns for each level
                        if per_level:
                            dist_cols = st.columns(len(per_level))
                            for i, (level, count) in enumerate(per_level.items()):
                                pct = (count / total_listings * 100) if total_listings > 0 else 0.0
                                dist_cols[i].metric(level.title(), f"{pct:.1f}%", delta_color="off")
                        else:
                            st.caption("No level distribution available")
                else:
                    st.info("No data available for this topic.")
            
            # Remaining tabs: Job Levels
            for i, level_name in enumerate(level_names):
                with tabs[i + 1]:
                    level_info = levels_data[level_name]
                    counts = level_info.get("counts", {})
                    listings_count = level_info.get("listings", 0)
                    matched_count = level_info.get("matched", 0)
                    
                    if counts:
                        st.altair_chart(make_chart(counts, listings_count, limit), use_container_width=True)
                        
                        c1, c2 = st.columns(2)
                        c1.metric("Listings at this level", listings_count)
                        c2.metric("Matched Keywords", matched_count)
                    else:
                        st.warning(f"No keywords found for {level_name} level.")
            
            st.divider()

st.set_page_config(page_title="Job Market Analysis", page_icon="💡", layout="wide")
st.title("💡 Job Market Analysis")
st.space()

# --- Sidebar: Session Selection ---
with st.sidebar:
    index: dict[int, str] = db.get_index()
    
    if not index:
        st.warning("No sessions found. Run the scraper via CLI first.")
        st.stop()

    current_id = st.session_state.get('current_session')
    default_index = list(index.keys()).index(current_id) if current_id in index else 0

    selected_session: int = st.selectbox(
        label="Sessions available",
        options=index.keys(),
        format_func=lambda option_id: f"{option_id}. {index[option_id]}",
        index=default_index
    )

    available_topics = sorted(list(topic_loader.get_available()))
    selected_topic_titles = []
    
    st.text("\n\n")
    with st.expander(label="Topics available", expanded=False):
        for topic in available_topics:
            if st.checkbox(topic, value=True, key=f"topic_{topic}"):
                selected_topic_titles.append(topic)

    st.text("\n")
    top_n = st.number_input(label="Top Keywords to Display", min_value=5, max_value=100, value=20, step=1)

    if st.button("Load Session", type="primary"):
        session = db.get_session(selected_session)
        st.session_state['current_session'] = session
        st.session_state['topics'] = topic_loader.load(set(selected_topic_titles))
        st.rerun()
    
    st.divider()
    st.markdown("""
    ### ℹ️ About this Dashboard
    This tool visualizes job market data trends and statistics.

    **How to use:**
    1. **Select a Session:** Choose a dataset from the dropdown above.
    2. **Filter Topics:** Toggle specific topics to refine your analysis.
    3. **Load Data:** Click 'Load Session' to generate the report.
    
    **Sections:**
    - **Overview:** General metrics and keyword popularity.
    - **Job Levels:** Breakdown of data by seniority (Junior, Senior, etc.).
    """)

if 'current_session' in st.session_state:
    session = st.session_state['current_session']
    topics = st.session_state.get('topics', [])
    processor = JobspyProcessor(session, topics)
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
    st.info("👈 Please select a session and the desired topics from the sidebar to view results.")
    st.stop()