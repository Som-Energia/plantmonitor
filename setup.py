#!/usr/bin/env python
from setuptools import setup, find_packages

readme = open("README.md").read()

setup(
	name = "plantmonitor",
	version = "0.1",
	description =
		"Distributed energy plant monitoring application",
	author = "Som Energia SCCL",
	author_email = "info@somenergia.coop",
	url = 'https://github.com/Som-Energia/plantmonitor',
	long_description = readme,
	license = 'Affero General Public License v3 or later (AGPLv3+)',
	packages=find_packages(exclude=['*[tT]est*']),
	scripts=[
		],
	install_requires=[
            'pymodbus>=1.3.2',
            'influxdb>=2.17.0',
            'python_dateutil>=2.6.1',
            'yamlns>=0.6',
            'APScheduler>=3.6.0',
            'erppeek',
            'nose',
            'rednose',
            'Flask==1.1.2',
            'Flask-RESTful==0.3.8',
            'pony>=0.7.13',
            'fastapi',
	],
	include_package_data = True,
	test_suite = 'plantmonitor',
	classifiers = [
		'Programming Language :: Python',
		'Programming Language :: Python :: 3',
		'Topic :: Software Development :: Libraries :: Python Modules',
		'Intended Audience :: Developers',
		'Development Status :: 2 - Pre-Alpha',
		'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
		'Operating System :: OS Independent',
	],
)
