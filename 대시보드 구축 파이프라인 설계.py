from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os

# ==========================================
# [설정] 환경 변수
# ==========================================
GCP_PROJECT_ID = 'codeit-ping-project'      # GCP 프로젝트 ID
GCP_BUCKET_NAME = 'daily_ping_project'    # GCS 버킷 이름
BQ_DATASET = 'For_Looker'                # 빅쿼리 데이터셋 이름
LOCAL_PATH = '/opt/airflow/data'       # dashboard_master.csv가 있는 경로 (컨테이너 내부)
CONN_ID = 'codeit-ping-project'       # Airflow Connection ID

default_args = {
    'owner': 'data_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 1),
}

# ==========================================
# [함수] 더미 데이터 생성 (Python)
# ==========================================
def generate_log_file(**kwargs):
    # 1. 마스터 파일 읽기
    master_file = os.path.join(LOCAL_PATH, 'dashboard_master.csv')
    df = pd.read_csv(master_file)
    
    # 2. 활동 유저 선정 (40%)
    active_mask = np.random.rand(len(df)) < 0.4
    active_users = df[active_mask].copy()
    
    # 3. 델타 값 생성 (오늘의 활동량)
    active_users['delta_send'] = np.random.randint(0, 6, size=len(active_users))
    active_users['delta_recv'] = np.random.randint(0, 9, size=len(active_users))
    active_users['delta_ad'] = np.random.randint(0, 4, size=len(active_users))
    # 포인트 계산
    active_users['delta_point'] = (active_users['delta_send']*10 + 
                                   active_users['delta_recv']*5 + 
                                   active_users['delta_ad']*30)
    
    # 4. 신규 유저 생성 (100~501명)
    new_user_count = np.random.randint(100, 502)
    max_id = df['user_id'].max() if not df.empty else 1000000
    school_pool = df[['school_id', 'region']].drop_duplicates()
    
    new_users = []
    for i in range(new_user_count):
        school = school_pool.sample(1).iloc[0]
        new_users.append({
            'user_id': max_id + 1 + i,
            'school_id': school['school_id'],
            'region': school['region'],
            'delta_send': np.random.randint(0, 3),
            'delta_recv': np.random.randint(0, 5),
            'delta_ad': np.random.randint(0, 2),
            'delta_point': 100,
            'is_new': True
        })
    
    # 5. 합치기 및 저장
    df_log = pd.concat([
        active_users[['user_id', 'school_id', 'region', 'delta_send', 'delta_recv', 'delta_ad', 'delta_point']].assign(is_new=False),
        pd.DataFrame(new_users)
    ])
    
    # 파일명에 시간 포함
    filename = f"daily_log_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    filepath = os.path.join(LOCAL_PATH, filename)
    df_log.to_csv(filepath, index=False)
    
    # 다음 Task로 정보 넘기기
    kwargs['ti'].xcom_push(key='csv_filename', value=filename)
    kwargs['ti'].xcom_push(key='csv_filepath', value=filepath)

# ==========================================
# [DAG] 파이프라인 정의
# ==========================================
with DAG(
    'daily_analytics_update',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup=False,
    tags=['analytics', 'dashboard']
) as dag:

    # 1. 더미 데이터 생성
    t1_generate = PythonOperator(
        task_id='generate_dummy_data',
        python_callable=generate_log_file
    )

    # 2. GCS 업로드 (Bridge)
    t2_upload_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src="{{ ti.xcom_pull(task_ids='generate_dummy_data', key='csv_filepath') }}",
        dst="logs/{{ ti.xcom_pull(task_ids='generate_dummy_data', key='csv_filename') }}",
        bucket=GCP_BUCKET_NAME,
        gcp_conn_id=CONN_ID,
    )

    # 3. BigQuery Staging 적재
    t3_load_staging = GCSToBigQueryOperator(
        task_id='load_to_staging',
        bucket=GCP_BUCKET_NAME,
        source_objects=["logs/{{ ti.xcom_pull(task_ids='generate_dummy_data', key='csv_filename') }}"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.daily_log_staging",
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        skip_leading_rows=1,
        gcp_conn_id=CONN_ID,
    )

    # 4. User Master 업데이트
    sql_merge_user = f"""
        UPDATE `{GCP_PROJECT_ID}.{BQ_DATASET}.dashboard_master` SET is_active_today = false WHERE true;

        MERGE `{GCP_PROJECT_ID}.{BQ_DATASET}.dashboard_master` T
        USING `{GCP_PROJECT_ID}.{BQ_DATASET}.daily_log_staging` S
        ON T.user_id = S.user_id
        WHEN MATCHED THEN
          UPDATE SET 
            is_active_today = true,
            send_vote_count = T.send_vote_count + S.delta_send,
            receive_vote_count = T.receive_vote_count + S.delta_recv,
            ad_view_count = T.ad_view_count + S.delta_ad,
            point = T.point + S.delta_point,
            active_days_count = T.active_days_count + 1
        WHEN NOT MATCHED THEN
          INSERT (user_id, school_id, region, send_vote_count, receive_vote_count, ad_view_count, point, is_active_today, active_days_count)
          VALUES (S.user_id, S.school_id, S.region, S.delta_send, S.delta_recv, S.delta_ad, S.delta_point, true, 1);
    """
    t4_update_user = BigQueryInsertJobOperator(
        task_id='update_dashboard_master',
        configuration={"query": {"query": sql_merge_user, "useLegacySql": False}},
        location='US',
        gcp_conn_id=CONN_ID,
    )
    
    # 5. School Master 재집계
    sql_agg_school = f"""
        CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.dashboard_school` AS
        SELECT
            school_id,
            ANY_VALUE(region) as region,
            COUNT(user_id) as current_user_count,
            COUNTIF(is_active_today = true) as daily_active_count,
            COUNTIF(is_active_today = true) / COUNT(user_id) as daily_active_ratio
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.dashboard_master`
        GROUP BY school_id
    """
    t5_update_school = BigQueryInsertJobOperator(
        task_id='update_dashboard_school',
        configuration={"query": {"query": sql_agg_school, "useLegacySql": False}},
        location='US',
        gcp_conn_id=CONN_ID,
    )

    # 순서 연결
    t1_generate >> t2_upload_gcs >> t3_load_staging >> t4_update_user >> t5_update_school