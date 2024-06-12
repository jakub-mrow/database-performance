import time
import matplotlib.pyplot as plt
import numpy as np
import random
import pandas as pd
from sqlalchemy import create_engine, text
from performance_test import PerformanceTest
from faker import Faker


class JoinBetweenTest(PerformanceTest):
    def __init__(self, db_url: str, num_employees: int, num_departments: int):
        self.engine = create_engine(db_url)
        self.num_employees = num_employees
        self.num_departments = num_departments
        self.queries = [
            ("Join using BETWEEN", "SELECT * FROM employees e JOIN departments d ON e.department_id BETWEEN d.dept_id AND d.dept_id + 10")
        ]

    def generate_data(self):
        fake = Faker()
        employees = []
        departments = []

        # Generate department data
        for dept_id in range(1, self.num_departments + 1):
            departments.append({
                'dept_id': dept_id,
                'dept_name': fake.company(),
                'location': fake.city(),
                'budget': round(random.uniform(50000, 500000), 2)
            })

        # Generate employee data
        for emp_id in range(1, self.num_employees + 1):
            employees.append({
                'emp_id': emp_id,
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'department_id': random.randint(1, self.num_departments),
                'salary': round(random.uniform(30000, 120000), 2),
                'hire_date': fake.date_between(start_date='-10y', end_date='today')
            })

        return pd.DataFrame(employees), pd.DataFrame(departments)

    def measure_execution(self, query, connection):
        execution_time = None
        start_time = time.time()
        connection.execute(text(query))
        end_time = time.time()
        execution_time = end_time - start_time

        # Get the execution plan
        plan = connection.execute(text(f"EXPLAIN ANALYZE {query}")).fetchall()
        return execution_time * 1000, plan

    def execute(self):
        def parse_actual_time(plan):
            for index, line in enumerate(plan):
                if index == 0:
                    parts = line[0].split('actual time=')
                    time_part = parts[1].split('..')[1]
                    value = float(time_part.split()[0])
                    return value

        employees_df, departments_df = self.generate_data()
        employees_df.to_sql('employees', self.engine, if_exists='replace', index=False)
        departments_df.to_sql('departments', self.engine, if_exists='replace', index=False)

        join_methods = ["nestloop", "hashjoin", "mergejoin"]
        execution_results = []

        with self.engine.connect() as connection:

            for join_method in join_methods:
                # Enable the specific join method and disable others
                connection.execute(text(f"SET enable_{join_method} = on;"))
                for disabled_method in [m for m in join_methods if m != join_method]:
                    connection.execute(text(f"SET enable_{disabled_method} = off;"))

                for name, query in self.queries:
                    exec_time, plan = self.measure_execution(query, connection)
                    actual_time = parse_actual_time(plan)
                    execution_results.append((f"{name} {join_method}", exec_time, plan, join_method, actual_time))

        # Reset all join methods
            for method in join_methods:
                connection.execute(text(f"SET enable_{method} = on;"))

        # Print the plans
        for name, exec_time, plan, join_method, actual_time in execution_results:
            print("--------------------------------------------------")
            print(f"Query: {name}")
            print(f"Execution Time: {exec_time:.4f} ms")
            print(f"Explained Time: {actual_time:.4f} ms")
            print(f"Join Method: {join_method}")
            print("--------------------------------------------------")
            print("Execution Plan:")
            for line in plan:
                print(line[0])
            print("--------------------------------------------------")

        query_names = [result[0] for result in execution_results]
        execution_times = [result[1] for result in execution_results]
        explained_times = [result[4] for result in execution_results]
        

        self.purge_tables()

        self.plot_results(execution_times, query_names, f"Join Between Test ({self.num_employees})", join_methods, explained_times)
    
    def plot_results(self, execution_times: list, query_names: list, title: str, join_methods: list, actual_times: list):
        # Set up the figure
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Create a bar width
        width = 0.35  # width of the bars
        x = np.arange(len(query_names))  # the label locations
        
        # Plot execution times and actual times side by side
        bars1 = ax.bar(x - width/2, execution_times, width, label='Execution Time', color='blue')
        bars2 = ax.bar(x + width/2, actual_times, width, label='Explain Time', color='lightgreen')
        
        # Set titles and labels
        ax.set_xlabel('Query')
        ax.set_ylabel('Time (ms)')
        ax.set_title(title)
        ax.set_xticks(x)
        ax.set_xticklabels(query_names, rotation=45)
        ax.legend()
        
        plt.tight_layout()
        plt.show()


        
    def purge_tables(self):
        with self.engine.connect() as connection:
            connection.execute(text("DROP SCHEMA public CASCADE; CREATE SCHEMA public;"))
