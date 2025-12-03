"""
Performance monitoring utilities.
"""
import time
from functools import wraps
from typing import Callable
import psutil
import os


def measure_time(func: Callable) -> Callable:
    """
    Decorator to measure function execution time.
    
    Usage:
        @measure_time
        def my_function():
            pass
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        print(f"{func.__name__} took {duration:.2f} seconds")
        return result
    return wrapper


def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / 1024 / 1024
    return round(memory_mb, 2)


def get_cpu_usage():
    """Get current CPU usage percentage."""
    return psutil.cpu_percent(interval=1)


class PerformanceMonitor:
    """Context manager for monitoring performance."""
    
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.start_time = None
        self.start_memory = None
    
    def __enter__(self):
        self.start_time = time.time()
        self.start_memory = get_memory_usage()
        print(f"Starting {self.operation_name}...")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self.start_time
        memory_used = get_memory_usage() - self.start_memory
        
        print(f"Completed {self.operation_name}")
        print(f"  Duration: {duration:.2f}s")
        print(f"  Memory used: {memory_used:.2f}MB")
        print(f"  CPU usage: {get_cpu_usage()}%")
