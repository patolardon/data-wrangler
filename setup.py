from setuptools import find_packages, setup


setup(
    name="datawrangler",
    version="0.0.10",
    description="a framework to build spark pipeline and test them",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=["pyspark == 3.3.0"],
    extras_require={
        "dev": ["pytest>=7.4.3"],
    }
)