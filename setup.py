import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='SalesforceEinsteinAnalytics',
    version='0.0.1',
    author='Adam Hilgenkamp',
    author_email='ahilgie@gmail.com',
    description='Python package for working with the Einstein Analytics API',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/ahilgenkamp/SalesforceEinsteinAnalytics',
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
