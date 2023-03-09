"""Setup file for Cytomine Project Migrator

   Copyright (c) 2009-2023. Authors: see NOTICE file.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

from setuptools import find_packages, setup

REQUIRES_PYTHON = ">=3.8.0"
REQUIRED = [
    "joblib==1.1.0",
    "requests>=2.21.0",
    "cytomine-python-client>=2.8.3",
]

DEPENDENCY_LINKS = [
    "https://packagecloud.io/cytomine-uliege/Cytomine-python-client/pypi/simple/cytomine-python-client/"
]

about = {}
PROJECT_SLUG = "cytomineprojectmigrator"
with open(f"{PROJECT_SLUG}/__version__.py", "r", encoding="utf-8") as f:
    exec(f.read(), about)

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name=PROJECT_SLUG,
    version=about["__version__"],
    description=about["__description__"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    author=about["__author__"],
    author_email=about["__author_email__"],
    url=["__url__"],
    packages=find_packages(),
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    license=about["__license__"],
    install_requires=REQUIRED,
    dependency_links=DEPENDENCY_LINKS,
    python_requires=REQUIRES_PYTHON,
)
