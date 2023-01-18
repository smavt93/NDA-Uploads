import pandas as pd
import numpy as np
import streamlit as st
from itertools import islice
from datetime import date, datetime
import time
pd.options.mode.chained_assignment = None

ndar_crosswalk = "https://github.com/smavt93/NDA-Uploads/blob/main/ndar_subject01_crosswalk.xlsx?raw=true"
ndar_template = "https://raw.githubusercontent.com/smavt93/NDA-Uploads/main/ndar_subject01_template.csv"
wasi_crosswalk = "https://github.com/smavt93/NDA-Uploads/blob/main/wasi201_crosswalk.xlsx?raw=true"
wasi_template = "https://github.com/smavt93/NDA-Uploads/raw/main/wasi201_template.csv"
demo01_crosswalk = "https://github.com/smavt93/NDA-Uploads/blob/main/demo01_crosswalk.xlsx?raw=true"
demo01_template = "https://github.com/smavt93/NDA-Uploads/raw/main/demo01_template.csv"
matrics01_crosswalk = "https://github.com/smavt93/NDA-Uploads/blob/main/matrics01_crosswalk.xlsx?raw=true"
matrics01_template = "https://github.com/smavt93/NDA-Uploads/raw/main/matrics01_template.csv"
topf01_crosswalk = ""
topf01_template = ""
today = date.today()
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
    st.markdown('- "QC" will also check the overall RC database for any imputations or missing data! (Would recommend doing this step first before creating docs).')
    st.markdown('### Regardless of what you choose, you will always need to have the previous upload docs on hand for this dashboard to work.')

@st.cache
def convert_df(df):
    return df.to_csv().encode('utf-8') 

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
        st.markdown("### Before you get started")
        st.markdown("1. Files you will need are (1) the full db from RC, (2) the previous upload document, and (3) the phenotype export from Wil.")
        st.markdown("2. After files are processed you should proceed to the QC portion of the dashboard to finalize the document.")
        col1, col2 = st.columns(2)
        with col1:
            full_database = st.file_uploader("Full RC data export:", key = "ndar")
        with col2:
            previous_ndar = st.file_uploader("Previous ndar_subject01 Upload doc:")
        if full_database is not None:
            if previous_ndar is not None:
                phenotype_file = st.file_uploader("Phenotype Crosswalk [EXCEL]")
                full_db = pd.read_csv(full_database)

                # Creating a list of excluded subjects that is adaptable
                reliability_db = full_db[full_db['subject_id'].str.contains('R')] # Removing the reliability subjects
                reliability_list = reliability_db['subject_id'].tolist()
                
                # Exclusion princples: visit_1_arm_1, doesn't have a GUID or Pseudoguid and is excluded (47)
                excluded_subject_db_1 = full_db.loc[(full_db['redcap_event_name'] == 'visit_1_arm_1')]
                excluded_subject_db_2 = excluded_subject_db_1.loc[((excluded_subject_db_1['guid'].isna()) & (excluded_subject_db_1['pseudoguid'].isna()))]
                excluded_subject_db_3 = excluded_subject_db_2.query("dx1 in [47] or subject_id in ['10055-20', '10178-20']")
                excluded_subject_filter_list = excluded_subject_db_3['subject_id'].values.tolist()
                final_excluded_subject_list = excluded_subject_filter_list + reliability_list # Final list of excluded subjects

                # Filtering for the desired datatype
                first_filter_db = full_db.loc[(full_db['scid_iscomplete'].notnull())]
                second_filter_db = first_filter_db.query("subject_id not in @final_excluded_subject_list")

                # Renaming columns and pulling the desired columns
                # Getting things ready
                crosswalk_db = pd.read_excel(ndar_crosswalk)
                rc_columns_to_change = crosswalk_db.rc_rename_columns.values.tolist()
                cleaned_rc_columns_to_change = [x for x in rc_columns_to_change if str(x) != 'nan']
                nda_columns_to_change = crosswalk_db['nda_column_names'].values.tolist()
                cleaned_nda_columns_to_change = [x for x in nda_columns_to_change if str(x) != 'nan']
                rc_columns_to_pull = crosswalk_db['rc_columns_to_pull'].values.tolist()
                crosswalk_dict = {cleaned_rc_columns_to_change[i]: cleaned_nda_columns_to_change[i] for i in range(len(cleaned_rc_columns_to_change))}
                # Desired columns
                ndar_desired_columns = second_filter_db.loc[:, rc_columns_to_pull]
                # Renaming columns
                ndar_subjects = ndar_desired_columns.rename(columns = crosswalk_dict)

                # Filling na items with -8
                ndar_subjects.fillna(-8,inplace=True)

                # Using the pseudoguid values if subject has no guid but does have pseudoguid data # for this dataset it doesn't apply but a good check regardless
                ndar_subjects['subjectkey'] = ndar_subjects.apply(lambda row: row['pseudoguid'] if row['subjectkey'] == -8 else row['subjectkey'], axis = 1)

                # Converting the sex column into NDA format # 0 --> M, 1 --> F
                def cat1(row):
                    if row['sex'] == 0:
                        return 'M'
                    if row['sex'] == 1:
                        return 'F'

                ndar_subjects['sex'] = ndar_subjects.apply(lambda row: cat1(row), axis = 1)

                #Converting the race items to the appropriate NDA format
                def cat2(row):
                    if row['race'] == 0:
                        return 'Declined to Answer'
                    if row['race'] == 1:
                        return 'American Indian/Alaska Native'
                    if row['race'] == 2:
                        return 'Asian'
                    if row['race'] == 3:
                        return 'Black or African American'
                    if row['race'] == 4:
                        return 'Hawaiian or Pacific Islander'
                    if row['race'] == 5:
                        return 'White'
                    if row['race'] == 6:
                        return 'Other Non-White'

                ndar_subjects['race'] = ndar_subjects.apply(lambda row: cat2(row), axis=1)

                # Converting the ethnicity items to the appropriate NDA format
                def cat3(row):
                    if row['ethnic_group'] == 0:
                        return 'Hispanic'
                    if row['ethnic_group'] == 1:
                        return 'Not Hispanic'

                ndar_subjects['ethnic_group'] = ndar_subjects.apply(lambda row: cat3(row), axis =1)

                # Getting the interview age column
                ndar_subjects['interview_date'] = pd.to_datetime(ndar_subjects['interview_date'])
                ndar_subjects['dob'] = pd.to_datetime(ndar_subjects['dob'])
                ndar_subjects['interview_age'] = ((ndar_subjects.interview_date - ndar_subjects.dob)/np.timedelta64(1, 'M'))
                ndar_subjects['interview_age'] = ndar_subjects['interview_age'].astype(int) # Still problems with dates but don't have time to figure them out now
                ndar_subjects['interview_date'] = ndar_subjects['interview_date'].dt.strftime("%m/%d/%y") # prevents the export from having the time component as well (not needed)

                # Adding columns that just have 'no'
                ndar_subjects['twins_study'] = 'No'
                ndar_subjects['sibling_study'] = 'No'
                ndar_subjects['family_study'] = 'No'
                ndar_subjects['sample_taken'] = 'No'

                # Dropping uneccessary columns
                ndar_subjects.drop(['pseudoguid', 'dob'], axis = 1, inplace=True)

                # Adding the phenotype items
                if phenotype_file is not None:
                    phenotype_db = pd.read_excel(phenotype_file, index_col = 'subject_id')
                    ndar_subjects.set_index('src_subject_id', inplace=True)

                    full_ndar_subject_db = ndar_subjects.merge(phenotype_db, how = 'outer', left_index=True, right_index=True)

                    # dropping uneccessary columns again
                    full_ndar_subject_db.drop(['n_diagnoses'], axis = 1, inplace=True)

                    # resetting index
                    full_ndar_subject_db.reset_index(inplace=True)

                    full_ndar_subject_db.rename(columns={'index':'src_subject_id'}, inplace=True)

                    # Setting index to guid to prepare for joined file
                    ndar_subject_db_prep = full_ndar_subject_db.set_index('subjectkey') 

                    # Creating the joined file
                    ndar_template_db = pd.read_csv(ndar_template, index_col = 'subjectkey')
                    final_ndar_subject_db = pd.concat([ndar_template_db, ndar_subject_db_prep])
                    
                    # Getting the legacy subject_ids
                    previous_upload = pd.read_csv(previous_ndar, skiprows=1)
                    previous_subject_id_list = previous_upload['src_subject_id'].values.tolist()
                    current_subject_id_list = final_ndar_subject_db['src_subject_id'].values.tolist()
                    legacy_list = [x for x in previous_subject_id_list if x not in current_subject_id_list]
                    legacy_db = previous_upload.query("src_subject_id in @legacy_list")
                    legacy_db.set_index('subjectkey', inplace=True)

                    # Replacing empty phenotype data that was filled in previous uploads with those values # Removing ids that don't have a correlate in the previous upload and no info in phenotype doc
                    
                    # Getting the subjects with empty phenotype section
                    empty_pheno_type_db = final_ndar_subject_db.loc[(final_ndar_subject_db['phenotype'].isna())]
                    empty_pheno_list = empty_pheno_type_db['src_subject_id'].values.tolist()
                    previous_upload_pheno = previous_upload.query("src_subject_id in @empty_pheno_list") # getting desired phenotype subject info
                    dropping_empty_pheno_db = final_ndar_subject_db.query("src_subject_id not in @empty_pheno_list") # removing the empty phenotype subjects
                    previous_upload_pheno.set_index('subjectkey', inplace=True)
                    # Joining two files to add the filled phenotype section
                    pheno_join_db = pd.concat([dropping_empty_pheno_db, previous_upload_pheno])
                    pheno_join_db.sort_values(by=['src_subject_id'], inplace = True)

                    # Creating final document
                    final_join_ndar_subject_db = pd.concat([pheno_join_db, legacy_db])
                    final_join_ndar_subject_db.sort_values(by=['src_subject_id'], inplace=True)
                    final_join_ndar_subject_db.index.name = 'subjectkey'
                    st.write(final_join_ndar_subject_db)
                    st.write("Number of Subjects: ", len(final_join_ndar_subject_db['src_subject_id']))
                    csv = convert_df(final_join_ndar_subject_db)
                    st.download_button("Download Data as a CSV", data = csv, file_name = f'ndar_subject_export {today}.csv', mime = 'text/csv')

    with tab2:
        st.header("Demo01")
        st.markdown("### Before you get started:")
        st.markdown("1. Files needed are (1) the full RC data export, (2) the previous upload document, and (3) the educ_yrs file from Wil.")
        st.markdown("2. After files are processed, you should proceed to the QC portion of the dashboard to finalize the document")
        col1, col2 = st.columns(2)
        with col1:
            full_database = st.file_uploader("Full RC data export:", key = "demo")
        with col2:
            previous_demo = st.file_uploader("Previous Demo01 Upload Doc:")
        if full_database is not None:
            if previous_demo is not None:
                educ_yrs_file = st.file_uploader("Educ_yrs document from Wil [EXCEL]")
                full_db = pd.read_csv(full_database)
                
                # Creating a list of excluded subjects that is adaptable
                reliability_db = full_db[full_db['subject_id'].str.contains('R')] # Removing the reliability subjects
                reliability_list = reliability_db['subject_id'].tolist()
                
                # Exclusion princples: visit_1_arm_1, doesn't have a GUID or Pseudoguid AND is excluded (47)
                excluded_subject_db_1 = full_db.loc[(full_db['redcap_event_name'] == 'visit_1_arm_1')]
                excluded_subject_db_2 = excluded_subject_db_1.loc[((excluded_subject_db_1['guid'].isna()) & (excluded_subject_db_1['pseudoguid'].isna()))]
                excluded_subject_db_3 = excluded_subject_db_2.query("dx1 in [47] or subject_id in ['10055-20', '10178-20']")
                excluded_subject_filter_list = excluded_subject_db_3['subject_id'].values.tolist()
                final_excluded_subject_list = excluded_subject_filter_list + reliability_list # Final list of excluded subjects

                # Filtering for the desired datatype
                first_filter_db = full_db.loc[(full_db['scid_iscomplete'].notnull())]
                second_filter_db = first_filter_db.query("subject_id not in @final_excluded_subject_list")
                
                # Renaming columns and pulling the desired columns
                # Getting things ready
                crosswalk_db = pd.read_excel(demo01_crosswalk)
                rc_columns_to_change = crosswalk_db.rc_rename_columns.values.tolist()
                cleaned_rc_columns_to_change = [x for x in rc_columns_to_change if str(x) != 'nan']
                nda_columns_to_change = crosswalk_db['nda_column_names'].values.tolist()
                cleaned_nda_columns_to_change = [x for x in nda_columns_to_change if str(x) != 'nan']
                rc_columns_to_pull = crosswalk_db['rc_columns_to_pull'].values.tolist()
                crosswalk_dict = {cleaned_rc_columns_to_change[i]: cleaned_nda_columns_to_change[i] for i in range(len(cleaned_rc_columns_to_change))}
                # Desired columns
                demo01_desired_columns = second_filter_db.loc[:, rc_columns_to_pull]
                # Renaming columns
                demo01_subjects = demo01_desired_columns.rename(columns = crosswalk_dict)

                # Filling na items with -8
                demo01_subjects.fillna(-8,inplace=True)

                # Using the pseudoguid values if subject has no guid but does have pseudoguid data # for this dataset it doesn't apply but a good check regardless
                demo01_subjects['subjectkey'] = demo01_subjects.apply(lambda row: row['pseudoguid'] if row['subjectkey'] == -8 else row['subjectkey'], axis = 1)

                #Converting the race items to the appropriate NDA format
                def cat1(row):
                    if row['race'] == 0:
                        return 'Declined to Answer'
                    if row['race'] == 1:
                        return 'American Indian/Alaskan Native'
                    if row['race'] == 2:
                        return 'Asian'
                    if row['race'] == 3:
                        return 'Black or African American'
                    if row['race'] == 4:
                        return 'Hawaiian or Pacific Islander'
                    if row['race'] == 5:
                        return 'White'
                    if row['race'] == 6:
                        return 'Unknown or not reported'

                demo01_subjects['race'] = demo01_subjects.apply(lambda row: cat1(row), axis=1)

                #Converting the relationship status to the appropriate NDA format
                def cat2(row):
                    if row['das1ms'] == 0:
                        return 'Never Married'
                    if row['das1ms'] == 1:
                        return 'Married'
                    if row['das1ms'] == 2:
                        return 'Separated'
                    if row['das1ms'] == 3:
                        return 'Divorced'
                    if row['das1ms'] == 4:
                        return 'Divorced and Remarried'
                    if row['das1ms'] == 5:
                        return 'Widowed'
                    if row['das1ms'] == 6:
                        return 'Widowed and Remarried'
                    if row['das1ms'] == 7:
                        return 'Living with SO'
                    if row['das1ms'] == 8:
                        return 'Other'

                demo01_subjects['das1ms'] = demo01_subjects.apply(lambda row: cat2(row), axis = 1)

                # Converting the integer representation of sex items to string characters
                def cat3(row):
                    if row['sex'] == 0:
                        return 'M'
                    if row['sex'] == 1:
                        return 'F'

                demo01_subjects['sex'] = demo01_subjects.apply(lambda row: cat3(row), axis = 1)

                # Handling the married column
                def cat4(row):
                    if row['das1ms'] == 'Married':
                        return 1
                    else:
                        return 0

                demo01_subjects['marital2'] = demo01_subjects.apply(lambda row: cat4(row), axis = 1)

                # Handling the significant other column
                def cat5(row):
                    if row['das1ms'] == 'Married' or row['das1ms'] == 'Living with SO':
                        return 1
                    else:
                        return 0

                demo01_subjects['sigother'] = demo01_subjects.apply(lambda row: cat5(row), axis = 1)

                # Handling the white race column
                def cat6(row):
                    if row['race'] == 'White':
                        return 1
                    else:
                        return 0

                demo01_subjects['white'] = demo01_subjects.apply(lambda row: cat6(row), axis = 1)

                # Handling the black race column
                def cat7(row):
                    if row['race'] == 'Black or African American':
                        return 1
                    else:
                        return 0

                demo01_subjects['black'] = demo01_subjects.apply(lambda row: cat7(row), axis = 1)

                # Handling the native race column
                def cat8(row):
                    if row['race'] == 'American Indian/Alaskan Native':
                        return 1
                    else:
                        return 0

                demo01_subjects['native'] = demo01_subjects.apply(lambda row: cat8(row), axis = 1)

                # Handling the asian race column
                def cat9(row):
                    if row['race'] == 'Asian':
                        return 'Yes'
                    else:
                        return 'No'

                demo01_subjects['asian'] = demo01_subjects.apply(lambda row: cat9(row), axis=1)

                # Handling the pacific race column
                def cat10(row):
                    if row['race'] == 'Hawaiian or Pacific Islander':
                        return 'Yes'
                    else:
                        return 'No'

                demo01_subjects['pacific'] = demo01_subjects.apply(lambda row:cat10(row), axis = 1)

                # Converting the subject's age at interview into months per NDA format
                demo01_subjects['interview_date'] = pd.to_datetime(demo01_subjects['interview_date'])
                demo01_subjects['dob'] = pd.to_datetime(demo01_subjects['dob'])
                demo01_subjects['interview_age'] = ((demo01_subjects.interview_date - demo01_subjects.dob)/np.timedelta64(1, 'M'))
                demo01_subjects['interview_age'] = demo01_subjects['interview_age'].astype(int) # Still problems with dates but don't have time to figure them out now
                demo01_subjects['interview_date'] = demo01_subjects['interview_date'].dt.strftime("%m/%d/%y") # prevents the export from having the time component as well (not needed)
                
                # Removing the pseudoguid and dob columns as those were just needed to get the desired column values. educ_yrs dropped to be replaced with correct values
                demo01_subjects.drop(['pseudoguid', 'dob', 'educ_yrs'], axis = 1, inplace= True)

                ########## Replacing incorrect edu_yrs with correct ones #######################
                if educ_yrs_file is not None:
                    educ_yrs_db = pd.read_excel(educ_yrs_file)
                    # Getting the correct subject IDs from the demo01 file to filter the educ_yr file
                    col_list = demo01_subjects['src_subject_id'].values.tolist()
                    complete_edu = educ_yrs_db.loc[educ_yrs_db['subject_id'].isin(col_list)]
                    educ_yrs_column = complete_edu[['subject_id','educ_yrs']]
                    
                    # Creating a file that has the correct educ_yrs info
                    correct_edu_col_db = pd.merge(demo01_subjects, educ_yrs_column, how = 'outer', left_on = ['src_subject_id'], right_on= ['subject_id'])
                    correct_edu_col_db.drop(['subject_id'], axis = 1, inplace=True)

                    # Setting the index column in order to facilitate the concat with template file
                    correct_edu_col_db_prep = correct_edu_col_db.set_index('subjectkey')
                    
                    # Join file created to get data in desired format
                    demo01_template_db = pd.read_csv(demo01_template, index_col = 'subjectkey')
                    final_demo01_join_file = pd.concat([demo01_template_db,correct_edu_col_db_prep])
                    
                    # Getting the legacy subject_ids
                    previous_upload_demo = pd.read_csv(previous_demo, skiprows = 1)
                    previous_subject_id_list_demo = previous_upload_demo['src_subject_id'].values.tolist()
                    current_subject_id_list_demo = final_demo01_join_file['src_subject_id'].values.tolist()
                    legacy_list_demo = [x for x in previous_subject_id_list_demo if x not in current_subject_id_list_demo]
                    legacy_db = previous_upload_demo.query("src_subject_id in @legacy_list_demo")
                    legacy_db.set_index('subjectkey', inplace = True)
                    
                    # Creating final document
                    final_demo01_file = pd.concat([final_demo01_join_file,legacy_db])
                    final_demo01_file.sort_values(by=['src_subject_id'], inplace=True)
                    final_demo01_file = final_demo01_file.loc[:, ~final_demo01_file.columns.str.match('Unnamed')]
                    st.write(final_demo01_file)
                    st.write("Number of subjects:", len(final_demo01_file['src_subject_id']))
                    csv = convert_df(final_demo01_file)
                    st.download_button("Download Data as a CSV", data = csv, file_name=f'demo01_subject_export {today}.csv', mime = 'text/csv')
    
    with tab3:
        st.header("Wasi201")
        st.markdown("### Before you get started:")
        st.markdown("1. All you need is the full RC data export.")
        st.markdown("2. After files are processed, you should proceed to the QC portion of the dashboard to finalize the document")
        full_database = st.file_uploader("Full RC data export:", key = "wasi")
        additional_excluded_subject = st.text_input("Are there any additional subjects that you would like to exclude?")
        if full_database is not None:
            full_db = pd.read_csv(full_database)
            # Creating a list of excluded subjects that is adaptable
            reliability_db = full_db[full_db['subject_id'].str.contains('R')] # Removing the reliability subjects
            reliability_list = reliability_db['subject_id'].tolist()
            
            # Exclusion princples: visit_1_arm_1, doesn't have a GUID or Pseudoguid AND is excluded (47)
            excluded_subject_db_1 = full_db.loc[(full_db['redcap_event_name'] == 'visit_1_arm_1')]
            excluded_subject_db_2 = excluded_subject_db_1.loc[((excluded_subject_db_1['guid'].isna()) & (excluded_subject_db_1['pseudoguid'].isna()))]
            excluded_subject_db_3 = excluded_subject_db_2.query("dx1 in [47] or subject_id in ['10055-20', '10178-20']")
            excluded_subject_filter_list = excluded_subject_db_3['subject_id'].values.tolist()
            final_excluded_subject_list = excluded_subject_filter_list + reliability_list # Final list of excluded subjects

            # Filtering for the desired datatype
            first_filter_db_wasii = full_db.loc[((full_db['np_date'].notnull()) | (full_db['wasiii_data_complete'] == 2))]
            # Removing 752-22 as data will never be recovered # no wasii data collected for 424-21 # Making it flexible in case someone wants to exclude more subjects
            second_filter_db_wasii = first_filter_db_wasii.query("subject_id not in @final_excluded_subject_list and subject_id not in ['10424-21','10752-22', @additional_excluded_subject]") 

            # Renaming columns and pullinf the desired columns
            crosswalk_db = pd.read_excel(wasi_crosswalk)

            rc_columns_to_change = crosswalk_db.rc_rename_columns.values.tolist()
            cleaned_rc_columns_to_change = [x for x in rc_columns_to_change if str(x) != 'nan']
            nda_columns_to_change = crosswalk_db['nda_column_names'].values.tolist()
            cleaned_nda_columns_to_change = [x for x in nda_columns_to_change if str(x) != 'nan']
            rc_columns_to_pull = crosswalk_db['rc_columns_to_pull'].values.tolist()
            crosswalk_dict = {cleaned_rc_columns_to_change[i]: cleaned_nda_columns_to_change[i] for i in range(len(cleaned_rc_columns_to_change))}
            # Desired columns
            wasii_desired_columns = second_filter_db_wasii.loc[:, rc_columns_to_pull]
            # Renaming columns
            wasii_subjects = wasii_desired_columns.rename(columns = crosswalk_dict)

            # Filling na items with -8
            wasii_subjects.fillna(-8,inplace=True)

            # Using the pseudoguid values if subject has no guid but does have pseudoguid data # for this dataset it doesn't apply but a good check regardless
            wasii_subjects['subjectkey'] = wasii_subjects.apply(lambda row: row['pseudoguid'] if row['subjectkey'] == -8 else row['subjectkey'], axis = 1)

            # Converting the sex column into NDA format # 0 --> M, 1 --> F
            def cat1(row):
                if row['sex'] == 0:
                    return 'M'
                if row['sex'] == 1:
                    return 'F'

            wasii_subjects['sex'] = wasii_subjects.apply(lambda row: cat1(row), axis = 1)

            # Converting the handedness column into NDA format # 0 --> L, 1 --> R
            def cat2(row):
                if row['handedness'] == 0:
                    return 'L'
                if row['handedness'] == 1:
                    return 'R'

            wasii_subjects['handedness'] = wasii_subjects.apply(lambda row: cat2(row), axis = 1)
            
            # Getting the interview age column
            wasii_subjects['interview_date'] = pd.to_datetime(wasii_subjects['interview_date'])
            wasii_subjects['dob'] = pd.to_datetime(wasii_subjects['dob'])
            wasii_subjects['interview_age'] = ((wasii_subjects.interview_date - wasii_subjects.dob)/np.timedelta64(1, 'M'))
            wasii_subjects['interview_age'] = wasii_subjects['interview_age'].astype(int) # Still problems with dates but don't have time to figure them out now
            wasii_subjects['interview_date'] = wasii_subjects['interview_date'].dt.strftime("%m/%d/%y") # prevents the export from having the time component as well (not needed)

            # Adding the 'visit' column that is not found in the RC db
            wasii_subjects['visit'] = 'neuropsych'

            # Dropping unecessary columns
            wasii_subjects.drop(['pseudoguid', 'dob'], axis = 1, inplace=True)

            # Setting index to facilitate concat with template file
            wasii_db_prep = wasii_subjects.set_index('subjectkey')

            # Creating the joined file
            wasii_template_db = pd.read_csv(wasi_template, index_col = 'subjectkey')
            wasii_db_final = pd.concat([wasii_template_db, wasii_db_prep])
            st.write(wasii_db_final)
            st.write("Number of Subjects:", len(wasii_db_final['src_subject_id']))
            csv = convert_df(wasii_db_final)
            st.download_button("Download Data as a CSV", data = csv, file_name = f'wasii_subject_export {today}.csv', mime = 'text/csv')

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
