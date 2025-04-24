from setuptools import find_packages, setup

setup(
    name="quickstart_etl",
    packages=find_packages(exclude=["quickstart_etl_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "boto3",
        "pandas",
        "requests",
        "matplotlib",
        "clickhouse-driver",
        "lz4>=4.0.0",
    ],
    extras_require={
        "dev": ["dagster-webserver", "pytest"],
        "compression": ["clickhouse-cityhash>=1.0.2"]
    },
    version="1.0.1",
)
