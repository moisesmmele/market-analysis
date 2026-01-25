from jobspy_processor import JobspyProcessor
from streamlit import session_state
from database import Database
import streamlit as st
import altair as alt

from session import Session

db = Database()

def make_chart(df, x_col, y_col):
    return alt.Chart(df).mark_bar().encode(
        x=alt.X(x_col, sort='-y', axis=alt.Axis(labelAngle=-45)),
        y=y_col,
        tooltip=[x_col, y_col]
    ).interactive()

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
        format_func=lambda option_id: index[option_id],
        index=default_index
    )

    if st.button("Load Session", type="primary"):
        session = db.get_session(selected_session)
        st.session_state['current_session'] = session
        st.rerun()

# --- Main Content ---
if 'current_session' in st.session_state:
    session = session_state['current_session']
    processor = JobspyProcessor(session)
    processor.process()
    st.write(processor.counted_keywords)
else:
    st.info("👈 Please select a session from the sidebar to view results.")
    st.stop()