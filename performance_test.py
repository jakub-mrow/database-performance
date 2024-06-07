from abc import ABC, abstractmethod

class PerformanceTest(ABC):
    @abstractmethod
    def generate_data(self, num_employees: int, num_departments: int):
        pass

    @abstractmethod
    def measure_execution(self, query: str) -> float:
        pass

    @abstractmethod
    def execute(self, queries: list, title: str):
        pass

    @abstractmethod
    def plot_results(self, execution_times: list, title: str):
        pass