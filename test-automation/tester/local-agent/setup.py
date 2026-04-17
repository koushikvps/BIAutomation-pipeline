from setuptools import setup, find_packages

setup(
    name="bi-test-agent",
    version="1.0.0",
    description="Local test agent for BI Automation Platform — drives Playwright on QA machine",
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=[
        "playwright>=1.40.0",
        "requests>=2.31.0",
        "openpyxl>=3.1.0",
    ],
    entry_points={
        "console_scripts": [
            "bi-test-agent=agent.cli:main",
        ],
    },
)
