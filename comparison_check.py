import pandas as pd
import streamlit as st
from itertools import islice
pd.options.mode.chained_assignment = None


st.title('VUMC NDA Upload Dashboard')
operation_selection = st.sidebar.selectbox("What would you like to do?", ["--", "Create Docs", "QC"])
if operation_selection == "--":
    st.markdown('## Welcome to the VUMC NDA Dashboard')
    st.markdown('---')
    st.markdown('This dashboard was created in hopes of streamlining the NDA upload process. This dashboard has the ability to create the desired files needed as well as to check created files to previous uploads.')
    st.markdown('On the side you should see a dropdown menu which gives you an option as to how you would like to use the dashboard.')
    st.markdown('- "--" is the default setting which just shows you this home page.')
    st.markdown('- "Create Docs" will give you the option to create any of the five NDA docs needed for the upload. Exlcuding the SCID data for now (1-17-23).')
    st.markdown('- "QC" will check previous uploads against your new upload file and will flag any discrepancies.')
    st.markdown('### Regardless of what you choose, you will always need to have the previous upload docs on hand for this dashboard to work.')

if operation_selection == "Create Docs":
    st.markdown("### Instructions")
    st.markdown("1. Click on the tab with the file you would like to create")
    st.markdown("2. Upload full RC data export from API pull.")
    st.markdown("3. Upload the previous upload for the desired file type.")
    st.markdown("4. Hit 'Create'")
    st.markdown("---")
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["ndar_subject01", "demo01", "wasi201", "topf01", "matrics01"])
   
    with tab1:
        st.header("Ndar_subjects01")
        col1, col2 = st.columns(2)
        with col1:
            full_database = st.file_uploader("Full RC data export:", key = "ndar")
        with col2:
            previous_ndar = st.file_uploader("Previous ndar_subject01 Upload doc:")
        if full_database is not None:
            full_db = pd.read_csv(full_database)

    with tab2:
        st.header("Demo01")
        col1, col2 = st.columns(2)
        with col1:
            full_database = st.file_uploader("Full RC data export:", key = "demo")
        with col2:
            previous_demo = st.file_uploader("Previous Demo01 Upload Doc:")
    
    with tab3:
        st.header("Wasi201")
        col1, col2 = st.columns(2)
        with col1:
            full_database = st.file_uploader("Full RC data export:", key = "wasi")
        with col2:
            previous_wasi = st.file_uploader("Previous Wasi201 Upload Doc:")
    
    with tab4:
        st.header("Topf01")
        col1, col2 = st.columns(2)
        with col1:
            full_database = st.file_uploader("Full RC data export:", key = "topf")
        with col2:
            previous_topf01 = st.file_uploader("Previous Topf01 Upload Doc:")
    
    with tab5:
        st.header("Matrics01")
        col1, col2 = st.columns(2)
        with col1:
            full_database = st.file_uploader("Full RC data export:", key = "matrics")
        with col2:
            previous_matrics01 = st.file_uploader("Previous Matrics01 Upload Doc:")
