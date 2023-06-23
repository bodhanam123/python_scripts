import copy
from derived_table_queries import BQ_DERIVED_TABLE_QUERIES
from recurring_queries import WORKFLOW_PARTITIONED_TABLE_QUERIES_FOR_SYNC

ai_mi_tables = ['ai_mock_interview_userquestionreport','ai_mock_interview_userquestionsubmission']

nocodb_cba_sales_tables = ['nc_53a5__college_dost_users_external_sources_data']

nxtbytes_tables = ["nxt_byte_byte", "nxt_byte_bytereaction", "nxt_byte_bytehashtag", "nxt_byte_hashtag", "nxt_byte_bytemultimedia", "nxt_byte_multimedia", "nxt_byte_userbytereaction", "nxt_byte_userbyte", "nxt_byte_userbytevideo", "nxt_byte_creator", "nxt_byte_creatorcategory", "nxt_byte_category", "nxt_byte_usercategory", "nxt_byte_userbytelastrecommendation","oauth2_provider_accesstoken"]

chatbot_tables = [
    'msg_messages'
]

topin_tables = [
    'nw_assessments_core_organisationassessment', 'nw_assessments_core_userorganisationassessment', 
    'nw_assessments_core_assessment', 'nw_assessments_core_assessmentlevel', 
    'nw_assessments_core_userentity','nkb_exam_exam', 'nkb_exam_examattempt', 'nkb_exam_examattemptquestion', 'nkb_exam_examquestion', 'nkb_exam_examconfig', 'nkb_exam_examquestionpoolconfig', 'nkb_question_questiontag', 'nkb_exam_questionpoolquestionstagconfig','nkb_exam_examscoringconfig','nkb_exam_examattemptextradetails'  
]

nw_sales_productization_tables = [
    'pre_sales_userapplicantverificationdetails', 'pre_sales_userapplicantnbfcapiresponsedetails', 
    'pre_sales_datacrdetails', 'pre_sales_userapplicantverificationsectionstatus',
    'pre_sales_userapplicantverificationdetails', 'pre_sales_checklist',
    'pre_sales_userchecklist',
    'pre_sales_userapplication',
    'pre_sales_userstepcompletiondetails', 'pre_sales_section', 'pre_sales_sectioncategory',
    'pre_sales_sectioncategorystep', 'pre_sales_cibildetails', 'pre_sales_employmentdetails','pre_sales_bankdetails_limited'
]

placement_tables = [
    'user_mock_interview_details'
]

exp_tables = [
    'nw_experiments_core_achievement',
    'nw_experiments_core_userachievement',
    'fly_high_challenge_targetunit', 'fly_high_challenge_usertargetunit', 
    'activation_challenge_userchallenge', 'activation_challenge_userchallengedailystat', 
    'activation_challenge_userchallengeprofile', 'activation_challenge_userweeklygoalstatus', 
    'fly_high_challenge_userchallengeprofile', 'fly_high_challenge_userchallengedailystat', 
    'fly_high_challenge_userchallenge', 'independence_day_challenge_userchallengeprofile', 
    'independence_day_challenge_userchallengedailystat', 'independence_day_challenge_userchallenge', 'nw_experiments_core_userdiscussionsdailystat','april_code_challenge_userchallengeprofile','april_code_challenge_userchallengedailystat','april_code_challenge_userchallenge','april_code_challenge_userreward'
]

learning_platform_tables = [
    'nkb_auth_userfeatureflag', 'nw_activities_useractivitygroupstreak',
    'django_swagger_utils_latencysummary', 'nkb_assignment_assignment', 'nkb_auth_userlanguage', 
    'nkb_auth_useronboardvideostatus', 'nkb_auth_userrole', 'nkb_auth_v2_devicedetails', 
    'nkb_auth_v2_enrollplanpartialaccessconfig', 'nkb_auth_v2_socialprofilestep', 'nkb_auth_v2_userbatchdetails', 
    'nkb_auth_v2_usercodingknowledgelevel', 'nkb_auth_v2_userdevicedetails', 'nkb_auth_v2_userenrollplan', 
    'nkb_auth_v2_userenrollplanpartialaccess', 'nkb_auth_v2_userfreetrailstatus', 'nkb_auth_v2_useridp', 
    'nkb_auth_v2_userlearningreschedulerequest', 'nkb_auth_v2_userprofilecompletiondetails', 
    'nkb_auth_v2_usertimecommitment', 'nkb_bookmark_userentitybookmark', 'nkb_calendar_usercalendar', 
    'nkb_certificate_certificate', 'nkb_certificate_usercertificate', 'nkb_coding_core_codeanalysis', 'nkb_coding_core_testcasedetails', 'nkb_coding_core_userdomain', 
    'nkb_coding_core_userrepository', 'nkb_coding_practice_userquestion', 
    'nkb_coding_practice_userquestionextradetails', 'nkb_coding_practice_userquestionresponse', 
    'nkb_coding_practice_userquestionruncode', 'nkb_discussions_comment', 'nkb_discussions_commentcreatedbyuser', 
    'nkb_discussions_commentextradetails', 'nkb_discussions_discussion', 'nkb_discussions_discussioncreatedbyuser', 
    'nkb_discussions_discussionset', 'nkb_discussions_discussionsetautoreply', 'nkb_discussions_discussionsetfaq', 
    'nkb_discussions_discussiontag', 'nkb_discussions_flag', 'nkb_discussions_reaction', 'nkb_exam_exam', 
    'nkb_exam_examattempt', 'nkb_exam_examattemptquestion', 'nkb_exam_examattemptquestionresponse', 
    'nkb_exam_examattemptquestionruncode', 'nkb_exam_examattemptquestionsavecode', 'nkb_exam_examconfig', 
    'nkb_exam_examquestion', 'nkb_exam_examquestionpoolconfig', 'nkb_exam_examscoringconfig', 'nkb_exam_questionconfig', 
    'nkb_feedback_userfeedbackdetails', 'nkb_fullstack_exam_examattempthtmlcodingquestionsavecode', 
    'nkb_gamification_wrapper_onboardstep', 'nkb_gamification_wrapper_usergamificationdetails', 
    'nkb_gamification_wrapper_useronboardstepdetails', 'nkb_guiding_question_guidingquestionconfig', 
    'nkb_guiding_question_guidingquestionroundconfig', 'nkb_guiding_question_userguidingquestionround', 
    'nkb_guiding_question_userguidingquestionroundquestion', 'nkb_interactive_video_interactivevideohotspot', 
    'nkb_interactive_video_interactivevideolearningresource', 'nkb_interactive_video_interactivevideolearningresourcemode', 
    'nkb_interactive_video_multimedia', 'nkb_jobs_job', 'nkb_jobs_jobextradetails', 'nkb_jobs_organisation', 
    'nkb_jobs_userjob', 'nkb_jobs_userplacementdetails', 'nkb_learning_path_growthcycletemplate', 
    'nkb_learning_path_learningpath', 'nkb_learning_path_learningpathresource', 'nkb_learning_path_offdatemodel', 
    'nkb_learning_path_usergrowthcycle', 'nkb_learning_path_userlearningpath', 'nkb_learning_path_userlearningpathresource', 
    'nkb_learning_path_userschedulepauserequest', 'nkb_learning_resource_learningresource', 
    'nkb_learning_resource_learningresourcemultimedia', 'nkb_learning_resource_learningresourceset', 
    'nkb_learning_resource_learningresourcesetlearningresource', 'nkb_learning_resource_multimedia', 
    'nkb_media_playback_usermultimediawatchedduration', 'nkb_notes_note', 'nkb_placement_support_placementsection', 
    'nkb_placement_support_placementsectionstep', 'nkb_placement_support_placementsectionstepresource', 
    'nkb_placement_support_userplacementsection', 'nkb_placement_support_userplacementstepchecklist', 
    'nkb_placement_support_userplacementstepinterview', 'nkb_placement_support_userplacementstepvideosubmission', 
    'nkb_placement_support_userplacementsupportaccess', 'nkb_placement_support_userresume', 
    'nkb_placement_support_uservideoevaluationresponse', 'nkb_placement_support_uservideosubmissionevaluation', 
    'nkb_placement_support_videosubmissionevaluation', 'nkb_plagiarism_userexamattemptplagiarism', 'nkb_projects_project', 
    'nkb_question_codeanalysisquestion', 'nkb_question_codingquestionuserresponse', 'nkb_question_htmlcodingquestion', 
    'nkb_question_htmlcodingquestiontestcasedetails', 'nkb_question_htmlcodingquestionuserresponse', 'nkb_question_idebasedcodingquestion', 
    'nkb_question_idebasedcodingquestiontestcasedetail', 'nkb_question_idebasedcodingquestionuserresponse', 'nkb_question_multimedia', 
    'nkb_question_option', 'nkb_question_question', 'nkb_question_questionguidedsolutionstep', 'nkb_question_questionset', 
    'nkb_question_questionsetquestion', 'nkb_question_questiontestcase', 'nkb_question_usercodingquestionstats', 
    'nkb_question_userresponse', 'nkb_question_userresponsemultimedia', 'nkb_question_userresponseoption', 
    'nkb_question_userresponsetext', 'nkb_resources_course', 'nkb_resources_program', 'nkb_resources_programcategory', 
    'nkb_resources_resource', 'nkb_resources_resourceenrollmentplan', 'nkb_resources_resourceparentresourcethroughmodel', 
    'nkb_resources_topic', 'nkb_resources_unit', 'nkb_resources_unittag', 'nkb_resources_usercoursecurrentstate', 
    'nkb_resources_userresource', 'nkb_resources_userresourceavailabilityconfig', 'nkb_support_mentor', 
    'nkb_support_usermentor', 'nkb_virtual_labs_idesession', 'nkb_webinar_webinarslot', 'nw_activities_activitygroup', 
    'nw_activities_activitygroupconfig', 'nw_activities_activitygroupconfig_completion_metric', 
    'nw_activities_completionmetric', 'nw_activities_useractivitygroupinstance', 
    'nw_activities_useractivitygroupinstancemetrictracker', 'nw_clubs_club', 'nw_clubs_clubuser', 
    'nw_resources_userresource', 'nw_resources_userresourcetransaction','nkb_virtual_labs_userdroplog', 'nkb_virtual_labs_resourceidesession','nkb_user_journey_userjobtrack','nkb_user_journey_userjobtracklog','nkb_user_journey_jobtrack','nkb_auth_userresourcerole', 'nkb_resources_userresourceversion', 'nkb_resources_courseversion','nkb_auth_v2_userenrollplanversionlog'
]


payments_tables = [
    'ib_installments_customizableinstallment', 'ib_installments_installment', 'ib_installments_installmentpayment', 
    'ib_payments_razorpayorderpaymentdetails', 'ib_payments_razorpaypaymentdetails', 'ib_payments_shopseorderdetails', 
    'ib_payments_shopseordertransactiondetails', 'ib_payments_shopseordertransactionwebhookcalldetails', 
    'otg_payments_changeprogramlog', 'otg_payments_changeprogramlog_user_orders', 'otg_payments_orderexceptionlog', 
    'otg_payments_orderinstallment', 'otg_payments_payment', 'otg_payments_product', 'otg_payments_razorpaypayment', 
    'otg_payments_razorpaypaymentlink', 'otg_payments_razorpaywebhookeventdatalog', 'otg_payments_transferprogramlog', 
    'otg_payments_userorder', 'otg_payments_userorderpayment', 'otg_payments_userorderproductaccess', 'otg_payments_usertags','user_order_emi_installment_details'
    'ib_payments_juspayorderdetails', 'ib_payments_juspayorderwebhookdetails', 'ib_payments_juspayordertransactiondetails'
]

ccbp_forms_tables = [
    'otg_forms_form', 'otg_forms_ibform', 'otg_forms_ibformsubmission', 'otg_forms_link', 'otg_forms_linkidaccesslog',
    'otg_forms_publicibformsubmission'
]

accounts_tables = [
    'accounts_hr_payout', 'accounts_hr_useraadharaddressdetails', 'accounts_hr_userassociation', 
    'accounts_hr_userbankaccount', 'accounts_hr_userpandetails', 'ib_user_accounts_useraccess', 
    'ib_user_accounts_userinvitecodes', 'ib_user_accounts_userpointslog', 'ib_user_accounts_userprofiledetails', 
    'ib_user_accounts_usertags', 'ib_users_useraddressdetails', 'ib_users_usercompanydetails', 
    'ib_users_usercontactdetails', 'ib_users_usercurrentprofessionaldetails', 'ib_users_userdegreedetails', 
    'ib_users_userguardiandetails', 'ib_users_userintermediatedetails', 'ib_users_userpreferredlanguages', 
    'ib_users_userprofessionalskills', 'ib_users_userprofile', 'ib_users_usersocialprofiledetails', 
    'ib_users_usersscdetails', 'ib_users_userworkexperiencedetails', 'user_info_usercontactdetails', 
    'user_info_userdetails'
]

whatsapp_tables = ['otg_whatsapp_whatsappmessagestatuslog']


nxtwave_tables = {
    'chat': copy.deepcopy(chatbot_tables),
    'placements': copy.deepcopy(placement_tables),
    'exp': copy.deepcopy(exp_tables),
    'otg_whatsapp': copy.deepcopy(whatsapp_tables),
    'otg': copy.deepcopy(learning_platform_tables),
    'otg_payments': copy.deepcopy(payments_tables),
    'forms': copy.deepcopy(ccbp_forms_tables),
    'accounts': copy.deepcopy(accounts_tables),
    'gas': ['ib_resource_userresourcepurchase'],
    'sales': copy.deepcopy(nw_sales_productization_tables),
    'topintech': copy.deepcopy(topin_tables),
    'nxtbytes':copy.deepcopy(nxtbytes_tables),
    'nocodb_cba_leads_prod':copy.deepcopy(nocodb_cba_sales_tables),
    'ai_mi':copy.deepcopy(ai_mi_tables)
}

nxtwave_restricted_tables = {
    'otg': {
        'oauth2_provider_accesstoken': ['user_id', 'created'],
    },
    'sales': {
        'pre_sales_userapplicantdetails': [
            'id', 'user_id', 'is_deleted', 
            'co_applicant_exists', 'relation_to_co_applicant', 
            'other_relation_to_co_applicant', 'co_applicant_id',
        ],
    },
}

ib_com_training_tables = {
    'ib_com_training': [
        'Reactions', 'Channels', 'ChannelMembers', 'PublicChannels'
    ],
}

ib_com_training_restricted_tables = {
    'ib_com_training': {
        'Sessions': ['Id', 'CreateAt', 'ExpiresAt', 'LastActivityAt', 'UserId', 'DeviceId'],
        'Posts': ['Id', 'CreateAt', 'UpdateAt', 'EditAt', 'DeleteAt', 'ChannelId', 'UserId'],
        'Audits': ['Id', 'CreateAt', 'UserId', 'Action', 'ExtraInfo'],
    }
}

nxtwave_dev_tables = copy.deepcopy(nxtwave_tables)
nxtwave_dev_tables.pop('placements')
nxtwave_dev_tables.pop('exp')
nxtwave_dev_tables.pop('otg_whatsapp')
nxtwave_dev_tables.pop('gas')
nxtwave_dev_tables.pop('chat')

nxtwave_dev_restricted_tables = copy.deepcopy(nxtwave_restricted_tables)

env_details = {
    'prod@nxtwave': {
        'msg_name': 'Nxtwave',
        'credential_path': "credentials.prod.json",
        'sql_instance_id': 'prod-query',
        'tables': nxtwave_tables,
        'restricted_tables': nxtwave_restricted_tables,
        'right_time': ["03:00", "06:30"],
        'continuous_replication': True,
        'always_on_sql_instance': True
    },
    'dev@nxtwave': {
        'msg_name': 'Nxtwave Dev',
        'credential_path': "credentials.dev.json",
        'sql_instance_id': 'dev-query',
        'tables': nxtwave_tables,
        'restricted_tables': nxtwave_restricted_tables,
        'right_time': ["01:00", "06:30"],
        'continuous_replication': True,
        'always_on_sql_instance': True
    },
    'ibcom@nxtwave': {
        'msg_name': 'iB Com CCBP',
        'credential_path': "credentials.prod.json",
        'sql_instance_id': 'prod-query',
        'tables': ib_com_training_tables,
        'restricted_tables': ib_com_training_restricted_tables,
        'right_time': ["00:00", "06:30"],
        'continuous_replication': True,
        'always_on_sql_instance': True,
        # 'dms_arn': "arn:aws:dms:ap-south-1:110916805156:task:WCU3CCKK4DEBZOR6EPXGKTVBYXVQXR74FKCG63Y"
    },
}

ready_environments = (
    'ibcom@nxtwave', 'prod@nxtwave', 'dev@nxtwave'
)

derived_queries_details = [
    {
        'dq_id': 1,
        'name': 'NxtWave',
        'queries': BQ_DERIVED_TABLE_QUERIES,
        'depends': ['prod@nxtwave', 'ibcom@nxtwave'],
        'bq_client_env': 'prod@nxtwave'
    }
]


def load_credentials_and_clients(environments=ready_environments):
    from google.cloud import bigquery
    from google.oauth2 import service_account
    from googleapiclient import discovery
    for env in environments:
        credential_path = env_details[env]['credential_path']
        credentials = service_account.Credentials.from_service_account_file(
            credential_path, scopes=[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        project_id = credentials.project_id
        bq_client = bigquery.Client(project=project_id, credentials=credentials)
        service = discovery.build('sqladmin', 'v1beta4', credentials=credentials, cache_discovery=False)
        # env_details[env]['credentials'] = credentials
        env_details[env]['project_id'] = project_id
        env_details[env]['bq_client'] = bq_client
        env_details[env]['service'] = service


datasets = {
    'prod@nxtwave': {
        'nkb_backend_otg_prod': 'otg',
        'otg_forms_backend_prod': 'forms',
        'ib_user_accounts_backend_prod': 'payments',
        'nw_exp_prod': 'exp',
        'ccbp_plcmt_prod': 'placements',
        'nw_sales_prdzn_prod': 'sales'
    }
}

all_db_names = {
    'prod@nxtwave': {
        'placements': 'ccbp_plcmt_prod',
        'exp': 'nw_exp_prod',
        'otg': 'nkb_backend_otg_prod',
        'forms': 'otg_forms_backend_prod',
        'payments': 'ib_user_accounts_backend_prod',
        'accounts': 'ib_user_accounts_backend_prod',
        'gas': 'gas_backend_prod',
        'otg_payments': 'onthegomodel_payments_backend_prod',
        'otg_whatsapp': 'whatsapp_prod',
        'sales': 'nw_sales_prdzn_prod',
        'chat': 'nw_bp_chatbot_prod'
    },
    'dev@nxtwave': {
        'accounts': 'ib_user_accounts_backend_gamma',
        'forms': 'otg_forms_backend_gamma',
        'otg': 'nkb_backend_otg_gamma',
        'otg_payments': 'onthegomodel_payments_backend_gamma',
        'sales': 'nw_sales_prdzn_beta'

    }
}

# Synced in dev but not used in bigquery dev:
# 'onthegomodel_payments_backend_beta'
# 'ib_user_accounts_backend_beta'
# 'otg_forms_backend_beta'
