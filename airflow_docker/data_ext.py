from datetime import datetime,timedelta
from airflow import DAG
from instagrapi import Client
from airflow.operators.python import PythonOperator
from airflow.models import TaskInstance 
from airflow.utils.dates import days_ago
import json

cli=Client()
def user_details_fetch(ti,**kwargs):
    details=['pk','username','full_name','is_private','profile_pic_url',
    'profile_pic_url_hd', 'is_verified', 'media_count', 'follower_count',
    'following_count','biography', 'external_url', 'account_type',
    'is_business', 'public_email', 'business_category_name','category_name']
    username=kwargs['dag_run'].conf.get('username')
    print(username)
    user_details=cli.user_info_by_username(username).dict()
    user_fetched={}
    for i in details:
        user_fetched[i]=user_details[i]
    print(user_fetched)
    ti.xcom_push(key='user_pk',value=int(user_fetched['pk']))
    ti.xcom_push(key='user_details',value=user_fetched)

def media_fetch(ti):
    user_pk=ti.xcom_pull(key='user_pk',task_ids=['user_details'])
    usr_mdia=cli.user_medias_v1(user_id=user_pk[0],amount=100)
    #media types of all media
    media_info=[]
    for i,j in enumerate(usr_mdia):
        if dict(j)['media_type']==8:
            for _ in range(len(dict(j)['resources'])):
                if dict(dict(j)['resources'][_])['media_type']==2:
                    media_info.append({'pk':dict(dict(j)['resources'][_])['pk'],'media_type':'video'})
                else:
                    media_info.append({'pk':dict(dict(j)['resources'][_])['pk'],'media_type':'photo'})
        elif dict(j)['media_type']==2 and dict(j)['product_type']=='feed':
            media_info.append({'pk':dict(usr_mdia[i])['pk'],'media_type':'video'})
        elif dict(j)['media_type']==2 and dict(j)['product_type']=='igtv':
            media_info.append({'pk':dict(usr_mdia[i])['pk'],'media_type':'igtv'})
        elif dict(j)['media_type']==2 and dict(j)['product_type']=='clips':
            media_info.append({'pk':dict(j)['pk'],'media_type':'reel'})
        else:
            media_info.append({'pk':dict(j)['pk'],'media_type':'photo'})
    ti.xcom_push(key='user_media',value=media_info)

def save_locally(ti):
    user_details=ti.xcom_pull(key='user_details',task_ids=['user_details'])
    user_medias=ti.xcom_pull(key='user_media',task_ids=['user_media_details'])
    media_info_dict={}
    for i,j in enumerate(user_medias[0]):
        media_info_dict[i]=j
    complete_info_dict={"user_details":user_details[0],"media_fetched":media_info_dict}
    with open("saved/complete.json",'w') as f:
        json.dump(complete_info_dict,f,indent=6)

    
def _on_dag_run_fail(context):
    print("***DAG failed!! do something***")
    print(f"The DAG failed because: {context['reason']}")
    print(context)
def _alarm(context):
    print("** Alarm Alarm!! **")
    task_instance: TaskInstance = context.get("task_instance")
    print(f"Task Instance: {task_instance} failed!")


args = {
  'owner': 'animesh',
  'depends_on_past': False,
  'start_date': datetime(2022,10,21),
  'email': ['xyz@gmail.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 3,
  "on_failure_callback": _alarm,
  'retry_delay':timedelta(seconds=10),
  'schedule_interval': None,
  'provide_context': True
}

with DAG(
    dag_id="extraction_pipeline",
    default_args=args,
    catchup=False,
    on_failure_callback=_on_dag_run_fail,
    dagrun_timeout=timedelta(minutes=5),
) as dag:
  t1 = PythonOperator(
    task_id='user_details',
    python_callable=user_details_fetch,
    on_retry_callback=user_details_fetch
    )

  t2=PythonOperator(
    task_id='user_media_details',
    python_callable=media_fetch,
    on_retry_callback=media_fetch
  )

  t3=PythonOperator(
    task_id='save_locally',
    python_callable=save_locally,
    on_retry_callback=save_locally
  )

t1>>t2>>t3