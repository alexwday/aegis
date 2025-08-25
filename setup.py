"""
Setup configuration for Aegis package.
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="aegis",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="AI-Powered Financial Data Assistant",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/alexwday/aegis",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.11",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "aegis-web=aegis.web_interface:main",
        ],
    },
    include_package_data=True,
    package_data={
        "aegis": [
            "model/prompts/**/*.yaml",
            "utils/ssl/*.cer",
        ],
    },
)