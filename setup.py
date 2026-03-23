from setuptools import find_packages, setup

setup(
    name="fetchm",
    version="0.1.11",
    author="Tasnimul Arabi Anik",
    author_email="arabianik987@gmail.com",
    description="A Python tool for fetching bacterial genome metadata and sequences.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Tasnimul-Arabi-Anik/fetchM",
    packages=find_packages(),
    scripts=["bin/fetchm", "bin/fetchM", "bin/fetchM-seq"],
    install_requires=[
        "pandas>=2.0",
        "requests>=2.31",
        "xmltodict>=0.13",
        "matplotlib>=3.7",
        "seaborn>=0.13",
        "scipy>=1.11",
        "tqdm>=4.66",
        "plotly>=5.20",
        "kaleido>=0.2.1",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
)
