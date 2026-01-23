import streamlit as st
import pandas as pd
import altair as alt
import io

import jobspy_df_processor
from database import DatabaseManager
import json

db = DatabaseManager()


def make_chart(df, x_col, y_col):
    return alt.Chart(df).mark_bar().encode(
        x=alt.X(x_col, sort='-y', axis=alt.Axis(labelAngle=-45)),
        y=y_col,
        tooltip=[x_col, y_col]
    ).interactive()

@st.cache_data(show_spinner=False)
def load_session_jobs(session_id):
    return db.get_jobs_by_session(session_id)

@st.cache_data(show_spinner=False)
def cached_process_session(jobs_df):
    """
    Enriches the dataframe using the data_processor module.
    This step is expensive and should be cached.
    """
    return data_processor.process_dataframe(jobs_df)

st.set_page_config(page_title="Job Scraper Viewer", page_icon="💀", layout="wide")
st.title("💀 Job Scraper Viewer")

# --- Sidebar: Session Selection ---
with st.sidebar:
    st.header("📂 Sessions")
    sessions = db.get_all_sessions()
    
    if sessions.empty:
        st.warning("No sessions found. Run the scraper via CLI first.")
        st.stop()
    
    # Create a nice label for the dropdown
    def format_session_label(row):
        try:
            meta = json.loads(row['meta'])
            term = meta.get('term', 'Unknown')
            loc = meta.get('location', 'Unknown')
        except:
            term = "N/A"
            loc = "N/A"
        return f"{row['id']} - {row['datetime_start']} | {term} ({loc})"

    # Dictionary for lookup
    session_options = {format_session_label(row): row['id'] for _, row in sessions.iterrows()}
    
    selected_label = st.selectbox("Select Session", options=list(session_options.keys()))
    selected_session_id = session_options[selected_label]
    
    top_n = st.number_input("Top X Keywords", min_value=5, max_value=100, value=30, step=5)
    
    if st.button("Load Session", type="primary"):
        st.session_state['current_session_id'] = selected_session_id
        st.rerun()

# --- Main Content ---
if 'current_session_id' in st.session_state:
    session_id = st.session_state['current_session_id']
    
    # Retrieve metadata for the current session to get termo/local
    current_session_row = sessions[sessions['id'] == session_id].iloc[0]
    try:
        meta = json.loads(current_session_row['meta'])
        termo = meta.get('term', 'Unknown')
        local = meta.get('location', 'Unknown')
    except:
        termo = "Session"
        local = str(session_id)

    with st.spinner("Loading and processing data..."):
        # 1. Load Raw Data
        raw_jobs = load_session_jobs(session_id)
        
        # 2. Transform / Enrich (Cached)
        processed_jobs = cached_process_session(raw_jobs)
        
        st.session_state['jobs'] = processed_jobs
        st.success(f"Loaded and processed {len(processed_jobs)} jobs from Session {session_id} ({termo} in {local})")
else:
    st.info("👈 Please select a session from the sidebar to view results.")
    st.stop()

# If we are here, 'jobs' in st.session_state should already be processed
if 'jobs' in st.session_state and not st.session_state['jobs'].empty:
    pass # Continue to existing logic
else:
     if 'jobs' in st.session_state and st.session_state['jobs'].empty:
        st.warning("Selected session has no jobs.")


if 'jobs' in st.session_state and not st.session_state['jobs'].empty:
    jobs = st.session_state['jobs']
    try:
        # Download Button
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            jobs.to_excel(writer, index=False)
        
        st.download_button(
            label="📥 Baixar Dados (XLSX)",
            data=buffer.getvalue(),
            file_name=f"vagas_{termo}_{local}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        # 2. Filter Logic
        if 'description' not in jobs.columns:
            st.warning("⚠️ Nenhuma descrição encontrada para processar.")
        else:
            # Check if processing happened
            if 'seniority_found' not in jobs.columns:
                 st.error("Data processing failed (missing columns).")
                 st.stop()

            # Identify all unique found seniorities for the filter
            all_found_seniorities = sorted(list(set([item for sublist in jobs['seniority_found'] for item in sublist])))
            
            # --- Filter UI ---
            st.markdown("---")
            # Default to all found
            selected_seniorities = st.multiselect(
                "🔍 Filtrar por Senioridade (apenas encontradas)",
                options=all_found_seniorities,
                default=all_found_seniorities
            )
            
            # Filter Data
            if not selected_seniorities:
                # UX: If nothing selected, show ALL (Reset behavior)
                mask = [True] * len(jobs)
                st.caption("ℹ️ Nenhum filtro selecionado. Mostrando todas as vagas.")
            else:
                # Row matches if its seniority_found has overlap with selected_seniorities
                mask = jobs['seniority_found'].apply(lambda x: any(s in selected_seniorities for s in x))

            filtered_jobs = jobs[mask]
            
            if len(filtered_jobs) == 0:
                st.warning("⚠️ Nenhuma vaga corresponde ao filtro selecionado.")
            else:
                st.info(f"Mostrando {len(filtered_jobs)} vagas filtradas de {len(jobs)}.")
                
                # 3. Aggregate Stats on the Filtered Subset
                df_backend, df_frontend, df_language, df_general, df_sen = data_processor.aggregate_keywords(filtered_jobs)

            # --- 3. Main Results (Categorized) ---
            st.markdown("---")
            
            # Row 1: Languages & Backend
            c1, c2 = st.columns(2)
            with c1:
                st.subheader("🗣️ Languages")
                st.altair_chart(make_chart(df_language.head(top_n), "Termo", "Freq"), use_container_width=True)
                st.dataframe(df_language, use_container_width=True, height=300)
            
            with c2:
                st.subheader("⚙️ Backend & DB")
                st.altair_chart(make_chart(df_backend.head(top_n), "Termo", "Freq"), use_container_width=True)
                st.dataframe(df_backend, use_container_width=True, height=300)

            # Row 2: Frontend & General
            c3, c4 = st.columns(2)
            with c3:
                st.subheader("🎨 Frontend")
                st.altair_chart(make_chart(df_frontend.head(top_n), "Termo", "Freq"), use_container_width=True)
                st.dataframe(df_frontend, use_container_width=True, height=300)
            
            with c4:
                st.subheader("🌍 General / Infra / Soft Skills")
                st.altair_chart(make_chart(df_general.head(top_n), "Termo", "Freq"), use_container_width=True)
                st.dataframe(df_general, use_container_width=True, height=300)

            # --- 4. Seniority ---
            st.markdown("---")
            st.subheader("📊 Seniority")
            c_sen_chart, c_sen_table = st.columns([2, 1])
            
            with c_sen_chart:
                if not df_sen.empty:
                    st.altair_chart(make_chart(df_sen, "Nível", "Contagem"), use_container_width=True)
                else:
                    st.info("N/A")

            with c_sen_table:
                st.dataframe(df_sen, use_container_width=True, height=400)

    except Exception as e:
        st.error(f"❌ Erro Detalhado: {str(e)}")
        st.exception(e)
