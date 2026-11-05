```py
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import random
 
def generate_random_number(**context):
    ti = context['ti']
    number = random.randint(1, 100)
    ti.xcom_push(key='random_number', value=number)
    print(f"Generated random number: {number}")
 
def check_even_odd(**context):
    ti = context['ti']
    number = ti.xcom_pull(task_ids='generate_number', key='random_number')
    result = "even" if number % 2 == 0 else "odd"
    print(f"The number {number} is {result}.")
 
with DAG(
    'random_number_checker',
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    description='A simple DAG to generate and check random numbers',
    catchup=False
) as dag:
 
    generate_task = PythonOperator(
        task_id='generate_number',
        python_callable=generate_random_number,
    )
 
    check_task = PythonOperator(
        task_id='check_even_odd',
        python_callable=check_even_odd,
    )
 
    generate_task >> check_task
```

1. Your task is to rewrite this DAG using the TaskFlow API. Your new DAG should:

- Use the @dag decorator to define the DAG

- Use the @task decorator for each task

- Utilize the TaskFlow API's automatic XCom feature for passing the random number between tasks

- Maintain the same functionality as the original DAG

2. Ensure that your new DAG:

- Generates a random number between 1 and 100

- Passes this number to the next task without explicitly using XCom methods

- Checks whether the number is even or odd

- Prints appropriate messages at each step

3. Test your DAG to make sure it works as expected.

- Add the dag to the dags/ folder (wait until it shows up on the UI, up to 5 mins)

- Unpause the DAG by turning on the toggle next to its name

- Check the logs of the check_even_odd task