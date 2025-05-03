from setuptools import setup, find_packages

setup(
    name="lancedb-browser",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "streamlit>=1.22.0",
        "pandas>=1.5.0",
    ],
    author="LanceDB Browser Team",
    author_email="info@example.com",
    description="A Streamlit application for browsing and exploring LanceDB tables",
    keywords="lancedb, streamlit, database, browser",
    url="https://github.com/yourusername/lancedb-browser",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)