# -*- coding: utf-8 -*-
import os

from invoke import task, run
from qcache import __version__ as qcache_version

docs_dir = 'docs'
build_dir = os.path.join(docs_dir, '_build')


@task
def test():
    run('python -m py.test -s -v -m "not benchmark"', pty=True)


@task
def benchmark():
    run('python -m py.test -s -v -m "benchmark"', pty=True)


@task
def coverage():
    run('python -m py.test --cov=qcache', pty=True)
    run('coverage report -m', pty=True)
    run('coverage html', pty=True)


@task
def flake8():
    run("flake8 qcache test")


@task
def clean():
    run("rm -rf build")
    run("rm -rf dist")
    run("rm -rf qcache.egg-info")
    clean_docs()
    print("Cleaned up.")


@task
def clean_docs():
    run("rm -rf %s" % build_dir)


@task
def browse_docs():
    run("open %s" % os.path.join(build_dir, 'index.html'))


@task
def build_docs(clean=False, browse=False):
    if clean:
        clean_docs()
    run("sphinx-build %s %s" % (docs_dir, build_dir), pty=True)
    if browse:
        browse_docs()


@task
def readme(browse=False):
    run('rst2html.py README.rst > README.html')


@task
def publish(test=False):
    """Publish to the cheeseshop."""
    if test:
        run('python setup.py register -r pypitest sdist upload -r pypitest')
    else:
        run("python setup.py register sdist upload")


@task
def build_image():
    run("sudo docker build -t tobgu/qcache:{version} .".format(version=qcache_version))
    run("sudo docker tag -f tobgu/qcache:{version} tobgu/qcache:latest".format(version=qcache_version))


@task
def push_image():
    run("sudo docker push tobgu/qcache:{version}".format(version=qcache_version))
    run("sudo docker push tobgu/qcache:latest")


def tag():
    run("git tag -fa v{version} -m 'v{version}'".format(version=qcache_version))

