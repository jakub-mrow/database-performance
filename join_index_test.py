import time
import matplotlib.pyplot as plt
import numpy as np
import random
import pandas as pd
from sqlalchemy import create_engine, text
from performance_test import PerformanceTest
from faker import Faker


class JoinIndexTest(PerformanceTest):
    def __init__(self, db_url: str, num_employees: int, num_departments: int):
        self.engine = create_engine(db_url)
        self.num_employees = num_employees
        self.num_departments = num_departments
        self.queries = [
            ("Join using ON", "SELECT * FROM employees e JOIN departments d ON e.department_id = d.dept_id")
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

    def create_indexes(self, connection):
        connection.execute(text("CREATE INDEX idx_employees_department_id ON employees(department_id);"))
        connection.execute(text("CREATE INDEX idx_departments_dept_id ON departments(dept_id);"))
        connection.execute(text("CREATE INDEX idx_departments_name ON departments(dept_name);"))

    def drop_indexes(self, connection):
        connection.execute(text("DROP INDEX IF EXISTS idx_employees_department_id;"))
        connection.execute(text("DROP INDEX IF EXISTS idx_departments_dept_id;"))
        connection.execute(text("DROP INDEX IF EXISTS idx_departments_name;"))

    def measure_execution(self, query, connection):
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
            # Run queries without indexes
            for join_method in join_methods:
                connection.execute(text(f"SET enable_{join_method} = on;"))
                for disabled_method in [m for m in join_methods if m != join_method]:
                    connection.execute(text(f"SET enable_{disabled_method} = off;"))

                for name, query in self.queries:
                    exec_time, plan = self.measure_execution(query, connection)
                    actual_time = parse_actual_time(plan)
                    execution_results.append((f"{name} {join_method} without index", exec_time, plan, join_method, actual_time))

            # Create indexes
            
            self.create_indexes(connection)
            # Run queries with indexes
            for join_method in join_methods:
                connection.execute(text(f"SET enable_{join_method} = on;"))
                for disabled_method in [m for m in join_methods if m != join_method]:
                    connection.execute(text(f"SET enable_{disabled_method} = off;"))

                for name, query in self.queries:
                    exec_time, plan = self.measure_execution(query, connection)
                    actual_time = parse_actual_time(plan)
                    execution_results.append((f"{name} {join_method} with index", exec_time, plan, join_method, actual_time))

            self.drop_indexes(connection)

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

        # Separate execution times by join method and index presence
        without_index_times = [(result[0], result[1]) for result in execution_results if 'without index' in result[0]]
        with_index_times = [(result[0], result[1]) for result in execution_results if 'with index' in result[0]]

        self.purge_tables()

        self.plot_results(without_index_times, with_index_times)

    def plot_results(self, without_index_times, with_index_times):
    
        # Extracting the labels and times for plotting
        labels = [result[0].replace(' without index', '') for result in without_index_times]
        without_index_values = [result[1] for result in without_index_times]
        with_index_values = [result[1] for result in with_index_times]

        # Setting up positions for bars
        x = np.arange(len(labels))
        width = 0.35

        # Plotting the bars
        fig, ax = plt.subplots()
        bars1 = ax.bar(x - width/2, without_index_values, width, label='Without Index', color='blue')
        bars2 = ax.bar(x + width/2, with_index_values, width, label='With Index', color='lightgreen')

        # Adding labels, title, and legend
        ax.set_ylabel('Execution Time (ms)')
        ax.set_title(f'Execution Time with and without Indexes ({self.num_employees})')
        ax.set_xticks(x)
        ax.set_xticklabels(labels, rotation=45, ha='right')
        ax.legend()

        # Display the plot
        plt.tight_layout()
        plt.show()

        
        
    def purge_tables(self):
        with self.engine.connect() as connection:
            connection.execute(text("DROP SCHEMA public CASCADE; CREATE SCHEMA public;"))

