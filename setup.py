import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dbpredict_pipes",
    version="0.3.0",
    author="Bryan Perry",
    author_email="bryan.perry@fticonsulting.com",
    description="End-user specific interface to prepare data for processing and prediction by dbpredict",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/bperry12/dbpredict_pipes",
    packages=setuptools.find_packages(),
    include_package_data = True, 
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Healthcare Industry",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
    ],
    python_requires='>=3.7',
    install_requires=['pytest','datetime','python-dateutil',
                      'sqlalchemy','pandas','cx_Oracle',
                      'pathlib','freezegun','tables'],
    keywords = 'diabetes prediction complications machine learning',
    project_urls = {
        'FTI CHEP' : 'https://www.fticonsulting.com/industries/healthcare-and-life-sciences/economics-and-policy',
        'Source' : "https://github.com/bperry12/dbpredict_pipes",
    },
)