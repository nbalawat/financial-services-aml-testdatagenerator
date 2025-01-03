from setuptools import setup, find_packages

setup(
    name="aml_monitoring",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "faker==20.1.0",
        "pandas==2.1.3",
        "numpy==1.26.2",
        "python-dateutil==2.8.2",
        "pydantic==2.5.2",
        "sqlalchemy>=2.0.0",
        "psycopg2-binary",
        "neo4j",
        "python-dotenv",
        "asyncpg==0.30.0"
    ],
    python_requires=">=3.8",
    author="Your Name",
    author_email="your.email@example.com",
    description="AML Transaction Monitoring System",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Financial and Insurance Industry",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
