import os
from sqlalchemy import create_engine
from multiprocessing import Pool, cpu_count

engine = create_engine(os.environ["BUILD_ENGINE"])
