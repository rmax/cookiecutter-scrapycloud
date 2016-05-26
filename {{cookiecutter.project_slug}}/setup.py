# Automatically created by: shub deploy

from setuptools import setup, find_packages

setup(
    name         = '{{cookiecutter.project_slug}}',
    version      = '0.1.0',
    packages     = find_packages(),
    entry_points = {'scrapy': ['settings = {{cookiecutter.project_slug}}.settings']},
    # These scripts will be available  as "Periodic Jobs".
    scripts      = [
        'bin/archive-items.py',
    ],
)
