import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="iginx",
    version="0.5.0",
    author="Zi Yuan",
    author_email="ziy20@outlook.com",
    description="IginX Python Lin.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/thulab/IginX",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)