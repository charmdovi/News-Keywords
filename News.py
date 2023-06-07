from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1),
    'retries': 0,
}
test_dag = DAG(
    'southkoreanews',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)


def gen_bash_task(name: str, cmd: str, trigger="all_success"):
    """airflow bash task 생성
        - trigger-rules : https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#trigger-rules
    """
    bash_task = BashOperator(
        task_id=name,
        bash_command=cmd,
        trigger_rule=trigger,
        dag=test_dag
    )
    return bash_task

# 사전 실행 - 하둡 분산파일시스템(DFS) 및 잡 메니져(YARN) 
"""
$ cd $HADOOP_HOME
$ pwd
/home/tom/hadoop/hadoop-3.3.4
$ sbin/start-dfs.sh
Starting namenodes on [localhost]
Starting datanodes
Starting secondary namenodes [LAPTOP-19ETOF4R]
$ sbin/start-yarn.sh
Starting resourcemanager
Starting nodemanagers
$ jsp
zsh: command not found: jsp
$ jps
2594 Jps
1506 DataNode
2132 NodeManager
2007 ResourceManager
1383 NameNode
1738 SecondaryNameNode
"""

# Define the BashOperator task
HQL_PATH='/home/dokyung/code/etl/dags/News'

get_api = gen_bash_task("get.api", f"hive -f {HQL_PATH}/step-1-get-api.py")

make_table = gen_bash_task("make.table", f"hive -f {HQL_PATH}/step-2-make-table.hql")
                          
# Set task dependencies
get_api >> make_table
