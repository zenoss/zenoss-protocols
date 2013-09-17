from setuptools import setup, find_packages

# 'make build' will build the protobufs and copy them to the needed locations.

setup(
    name = "zenoss.protocols",
    version = "1.0.0a",
    packages = find_packages(),
    long_description=open('README.txt').read(),
    install_requires = [
        'setuptools',
        'httplib2',
        'zope.dottedname',
        'zope.interface',
        'zope.component',
        'Twisted',
        'amqplib',
        'txamqp',
        'urllib3>=1.3',
    ],
    include_package_data=True,
    # in the zenoss namespace
    namespace_packages = ['zenoss'],
    # metadata for upload to PyPI
    author = "Zenoss",
    author_email = "support@zenoss.com",
    description = "Protobufs and AMQP client for integrating with Zenoss.",
    license = "GPLv2 or later",
    keywords = "zenoss protocols",
    url = "http://www.zenoss.com/",
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'zenqdump = zenoss.protocols.scripts.zenqdump:main',
            'zenqpush = zenoss.protocols.scripts.zenqpush:main',
            'zenqload = zenoss.protocols.scripts.zenqload:main',
            'zenqdelete = zenoss.protocols.scripts.zenqdelete:main',
        ]
    },
    test_suite = "tests.suite.test_all"
)
