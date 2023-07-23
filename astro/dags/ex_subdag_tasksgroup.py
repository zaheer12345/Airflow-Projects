from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime


from airflow.utils.task_group import TaskGroup

with DAG ("ex_task_groups", 
          default_args={
              "owner":"airflow",
              "start_date": datetime(2023, 7, 22)
          })as dag :
    start = DummyOperator(task_id = "start")
    end = DummyOperator(task_id = "end")

# start >> a >> a1 >> b >> c >> d >> e >> f >> g >> end


    ## [SIMPLE SEQUENTIAL TASK GROUP]
with TaskGroup("A-A1", tooltip="Task Group for A & A1") as gr_1:

    a = DummyOperator(task_id = "Task_A")
    a1 = DummyOperator(task_id = "Task_A1")
    b = DummyOperator(task_id = "Task_B")
    c = DummyOperator(task_id = "Task_C")
    a >> a1


# start >> gr_1 >> d >> e >> f>> g >> end

    ## [Nested Task Group]

with TaskGroup("D-E-F", tooltip="Nested Task Group") as gr_2:
    d = DummyOperator(task_id ="Task_D")


    with TaskGroup("E-F-G", tooltip="Inner Nested Task Group") as gr_2:
        e = DummyOperator(task_id = "Task_E")
        f = DummyOperator(task_id = "Task_F")
        g = DummyOperator(task_id = "Task_G")
        e >> f
        e >> g

start >> gr_1 >> gr_2 >> end 