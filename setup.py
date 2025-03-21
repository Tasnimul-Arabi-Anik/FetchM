from setuptools import setup

setup(
    name="fetchM",
    version="0.1.0",
    author="Tasnimul Arabi Anik",
    author_email="arabianik987@gmail.com",
    description="A Python tool for fetching metadata for bacterial genomes.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Tasnimul-Arabi-Anik/fetchM",
    py_modules=["fetchM"],  # Specify the standalone script here
    install_requires=[
        "pandas",
        "requests",
        "xmltodict",
        "matplotlib",
        "seaborn",
        "scipy",
        "tqdm"
    ],
    entry_points={
        "console_scripts": [
            "fetchM=fetchM:main",  # Point to the script and function
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
)
